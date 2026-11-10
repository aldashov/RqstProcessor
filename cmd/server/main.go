package main

import (
	"context"
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

	"github.com/jackc/pgx/v5/pgxpool"
)

// инициализация БД
func db_init() (pCtx context.Context, pool *pgxpool.Pool, err error) {
	pCtx = context.Background()

	//Считываем строку подключения из переменных окружения
	connstr := os.Getenv("DATABASE_URL")

	//Если запускаем локально без Докера (переменная пуста)
	if connstr == "" {
		connstr = "postgres://postgres:your_password@localhost:5432/postgres?sslmode=disable"
	}

	pool, err = pgxpool.New(pCtx, connstr)
	if err != nil {
		fmt.Printf("[CTXINIT] There is error while creating pool %v", err.Error())
		return
	}

	//проверка соединения с БД
	err = pool.Ping(pCtx)
	if err != nil {
		fmt.Printf("[DBINIT] There is error while trying to connect to DataBase %v", err.Error())
		return pCtx, pool, err
	}

	return pCtx, pool, err
}

func main() {
	// Инициализируем структурированный текстовый логгер, пишущий в системный вывод (консоль)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Запуск сервиса обработки задач", slog.String("port", "8081"))

	// Инициализация контекста приложения с возможностью явной отмены
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	// Инициализация пула соединений СУБД
	_, pool, err := db_init()
	if err != nil {
		slog.Error("Критическая ошибка инициализации БД", "error", err)
		return
	}
	defer pool.Close()

	// Сборка слоев
	repo := database.NewRepo(pool)
	chnl_task := make(chan *service.Task, 20)
	var wg sync.WaitGroup

	// Запуск пула фоновых воркеров
	background.WorkerInit(chnl_task, &wg, appCtx, repo)

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

	// Закрываем канал задач
	close(chnl_task)
	slog.Info("Канал закрыт. Ожидание завершения обработки буфера...")

	// Ждем, пока воркеры полностью опустошат буфер (до 20 элементов)
	wg.Wait()
	slog.Info("Все фоновые процессы штатно завершены. Выключение приложения.")
}
