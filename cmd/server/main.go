package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"task_processor/internal/background"
	"task_processor/internal/database"
	"task_processor/internal/service"
	"task_processor/internal/web"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// инициализация БД
func db_init() (pCtx context.Context, db *gorm.DB, sqlDB *sql.DB, err error) {
	pCtx = context.Background()

	//Считываем строку подключения из переменных окружения
	connstr := os.Getenv("DATABASE_URL")

	//Если запускаем локально без Докера (переменная пуста)
	if connstr == "" {
		connstr = "postgres://postgres:your_password@localhost:5432/postgres?sslmode=disable"
	}

	db, err = gorm.Open(postgres.Open(connstr), &gorm.Config{})
	if err != nil {
		fmt.Printf("[CTXINIT] There is error while creating gorm db %v", err.Error())
		return pCtx, nil, nil, err
	}

	sqlDB, err = db.DB()
	if err != nil {
		fmt.Printf("[DBINIT] There is error while getting sql.DB %v", err.Error())
		return pCtx, db, nil, err
	}

	// проверка соединения с БД
	err = sqlDB.PingContext(pCtx)
	if err != nil {
		fmt.Printf("[DBINIT] There is error while trying to connect to DataBase %v", err.Error())
		return pCtx, db, sqlDB, err
	}

	return pCtx, db, sqlDB, nil
}

func main() {
	// Инициализируем структурированный текстовый логгер, пишущий в системный вывод (консоль)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Запуск сервиса обработки задач", slog.String("port", "8081"))

	// Инициализация контекста приложения с возможностью явной отмены
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	// Инициализация GORM и пула соединений СУБД
	_, db, sqlDB, err := db_init()
	if err != nil {
		slog.Error("Критическая ошибка инициализации БД", "error", err)
		return
	}
	defer sqlDB.Close()

	// Сборка слоев
	repo := database.NewRepo(db)
	chnl_task := make(chan *service.Task, 20)
	var wg sync.WaitGroup

	// Запуск пула фоновых воркеров
	background.WorkerInit(chnl_task, &wg, appCtx, repo)

	// Восстановление незавершённых задач из БД после предыдущего быстрого shutdown
	unfinishedTasks, err := repo.RecGetUnfinished(appCtx)
	if err != nil {
		slog.Error("Ошибка при восстановлении незавершённых задач из БД", "error", err)
	} else {
		slog.Info("Проверка незавершённых задач в БД при старте", slog.Int("count", len(unfinishedTasks)))
		if len(unfinishedTasks) > 0 {
			go func(tasks []*service.Task) {
				for _, t := range tasks {
					select {
					case chnl_task <- t:
					case <-appCtx.Done():
						return
					}
				}
			}(unfinishedTasks)
		}
	}

	// Инициализация слоя HTTP-хендлеров и роутера
	taskHandler := web.NewTaskHandler(repo, chnl_task)

	mux := http.NewServeMux()

	// Регистрируем эндпоинты
	// Этот путь отвечает за операции БЕЗ указания ID (Создание и Получение списка)
	mux.HandleFunc("/tasks", taskHandler.HandleCollection)

	// Этот путь отвечает за операции С УКАЗАНИЕМ ID (Просмотр одного и Удаление)
	// Наличие слэша на конце "/" для перехвата динамических UUID
	mux.HandleFunc("/tasks/", taskHandler.HandleSingle)

	// Эти пути для healthcheck
	mux.HandleFunc("/health", taskHandler.HealthCheck)
	mux.HandleFunc("/health/", taskHandler.HealthCheck)

	// Настройка HTTP-сервера с таймаутами защиты (Production-ready)
	srv := &http.Server{
		Addr:         ":8081",
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Перехват системных сигналов завершения (SIGINT / SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Асинхронный запуск HTTP-сервера, чтобы он не блокировал main-поток
	go func() {
		slog.Info("Сервер запущен через порт :8081")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("Критическая ошибка остановки сервера:", "error", err)
			appCancel() // Отменяем контекст, если сервер упал сам
		}
	}()

	// БЛОКИРОВКА main: Ждем сигнал остановки от ОС (Ctrl+C или docker stop)
	sig := <-sigChan
	slog.Info("Перехвачен сигнал", slog.String(sig.String(), "Инициирован Graceful Shutdown..."))

	// Мгновенно останавливаем HTTP-сервер (прекращаем прием новых запросов)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("Ошибка при принудительной остановке сервера:", "error", err)
	}
	slog.Info("Веб-сервер успешно остановлен. Новые запросы не принимаются")

	// Отменяем родительский контекст приложения
	appCancel()
	slog.Info("Контекст отменен. Ожидание завершения только активных воркеров...")

	// 3. Ждем завершения ТОЛЬКО тех задач, которые уже взяты в работу
	wg.Wait()

	// Закрываем канал задач
	close(chnl_task)
	slog.Info("Канал закрыт. Ожидание завершения обработки буфера...")
}
