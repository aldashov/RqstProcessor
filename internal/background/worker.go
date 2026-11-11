package background

import (
	"context"
	"log/slog"
	"sync"
	"task_processor/internal/service"
	"time"
)

const DefaultTaskDuration = 7 * time.Second

// ф-ия обработки одной задачи
func processTask(repo service.TaskRepo, task *service.Task, duration time.Duration) {
	task.UpdatedAt = time.Now()
	task.Status = "processing"

	if err := repo.RecUpdate(context.Background(), task); err != nil {
		slog.Error("Не удалось обновить статус запроса в базе данных",
			slog.String("id", task.Id),
			"error", err,
		)
		return
	}

	slog.Info("Запрос взят в фоновую обработку",
		slog.String("id", task.Id),
		slog.String("title", task.Title),
	)

	// задержка без прерывания при shutdown
	time.Sleep(duration)

	task.Status = "done"
	task.UpdatedAt = time.Now()

	if err := repo.RecUpdate(context.Background(), task); err != nil {
		slog.Error("Не удалось обновить статус запроса в базе данных",
			slog.String("id", task.Id),
			"error", err,
		)
		return
	}

	slog.Info("Запрос обработан",
		slog.String("id", task.Id),
		slog.String("title", task.Title),
	)
}

// ф-ия запуска Воркера
func WorkerRun(chnl <-chan *service.Task, wg *sync.WaitGroup, pCtx context.Context, repo service.TaskRepo) {
	_ = WorkerRunWithDuration(chnl, wg, pCtx, repo, DefaultTaskDuration)
}

// ф-ия запуска Воркера с указанием длительности обработки
func WorkerRunWithDuration(chnl <-chan *service.Task, wg *sync.WaitGroup, pCtx context.Context, repo service.TaskRepo, duration time.Duration) error {
	// завершение таска в WaitGroup при выходе из функции
	defer wg.Done()

	for {
		// 1. Приоритетная проверка отмены контекста ДО чтения следующей задачи из буфера
		if pCtx.Err() != nil {
			slog.Info("Завершение работы воркера по сигналу остановки (остаток очереди не вычитывается)",
				"reason", pCtx.Err(),
			)
			return pCtx.Err()
		}

		select {
		case <-pCtx.Done():
			// Контекст отменён: немедленно выходим, не трогая оставшиеся в буфере задачи
			slog.Info("Завершение работы воркера по сигналу остановки",
				"reason", pCtx.Err(),
			)
			return pCtx.Err()

		case task, ok := <-chnl:
			// Получение значения из канала
			if !ok {
				// Канал закрыт (все задачи закрыты)
				slog.Info("Все задачи обработаны, канал воркеров закрыт")
				return nil
			}

			// 2. Защита от псевдослучайного выбора select в Go:
			// если pCtx отменился одновременно с появлением задачи в буфере, в работу её НЕ берём
			if pCtx.Err() != nil {
				slog.Info("Сигнал остановки получен до старта задачи, задача оставлена в очереди",
					slog.String("id", task.Id),
				)
				return pCtx.Err()
			}

			// Доводим текущую взятую задачу до статуса done без обрыва по внутреннему select
			processTask(repo, task, duration)
		}
	}
}

// ф-ия инициализации воркеров
func WorkerInit(chnl <-chan *service.Task, wg *sync.WaitGroup, pCtx context.Context, repo service.TaskRepo) (err error) {
	// запуск трех воркеров
	for i := 0; i < 3; i++ {
		// увеличение счетчика заданий группы воркеров
		wg.Add(1)

		// запуск горутины Воркера
		go WorkerRun(chnl, wg, pCtx, repo)
	}

	return nil
}
