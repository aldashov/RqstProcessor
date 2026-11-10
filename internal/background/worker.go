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
	//завершение таска в WaitGroup при выходе из функции
	defer wg.Done()

	for {
		select {
		case <-pCtx.Done():
			// контекст отменен. дорабатываем все оставшиеся в буфере канала задачи до закрытия канала
			for task := range chnl {
				processTask(repo, task, duration)
			}
			slog.Info("Завершение работы воркера после обработки остатка очереди",
				"reason", pCtx.Err(),
			)
			return pCtx.Err()

		case task, ok := <-chnl:
			//получение значения из канала
			if !ok {
				//канал закрыт( все задачи закрыты)
				slog.Info("Все задачи обработаны, канал воркеров закрыт")
				return nil
			}

			// Доводим текущую задачу до статуса done без обрыва по внутреннему select
			processTask(repo, task, duration)
		}
	}
}

// ф-ия инициализации воркеров
func WorkerInit(chnl <-chan *service.Task, wg *sync.WaitGroup, pCtx context.Context, repo service.TaskRepo) (err error) {
	// запуск трех воркеров
	for i := 0; i < 3; i++ {
		//увеличение счетчика заданий группы воркеров
		wg.Add(1)

		//запуск горутины Воркера
		go WorkerRun(chnl, wg, pCtx, repo)
	}

	return nil
}
