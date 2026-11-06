package background

import (
	"context"
	"log/slog"
	"rqst_processor/internal/service"
	"sync"
	"time"
)

// ф-ия запуска Воркера
func WorkerRun(chnl <-chan *service.Request, wg *sync.WaitGroup, pCtx context.Context, repo service.RqstRepo) error {
	//завершение таска в WaitGroup при выходе из функции
	defer wg.Done()

	for {
		select {
		case <-pCtx.Done():
			// контекст отменен. проверка, остались ли еще таски в канале
			for rqst := range chnl {
				//обновление статуса записи запроса
				rqst.UpdatedAt = time.Now()
				rqst.Status = "processing"

				err := repo.RecUpdate(context.Background(), rqst)
				if err != nil {
					slog.Error("Не удалось обновить статус запроса в базе данных",
						slog.String("id", rqst.Id),
						"error", err,
					)
					continue
				}
				slog.Info("Запрос обработан",
					slog.String("id", rqst.Id),
					slog.String("title", rqst.Title),
				)

				//ожидание в 1 сек для эмуляции какой-то деятельности
				time.Sleep(1 * time.Second)
				//обновление статуса заявки
				rqst.Status = "done"
				rqst.UpdatedAt = time.Now()

				err = repo.RecUpdate(context.Background(), rqst)
				if err != nil {
					slog.Error("Не удалось обновить статус запроса в базе данных",
						slog.String("id", rqst.Id),
						"error", err,
					)
					continue
				}

				slog.Info("Запрос обработан",
					slog.String("id", rqst.Id),
					slog.String("title", rqst.Title),
				)
			}

			// контекст приложения отменен (Graceful Shutdown)
			slog.Error("Завершение работы по сигналу контекста",
				"error", pCtx.Err(),
			)
			return pCtx.Err()

		case rqst, ok := <-chnl:
			//получение значения из канала
			if !ok {
				//канал закрыт( все задачи закрыты)
				slog.Info("Все задачи обработаны, канал воркеров закрыт")
				return nil
			}

			//обновление статуса записи запроса
			rqst.UpdatedAt = time.Now()
			rqst.Status = "processing"

			err := repo.RecUpdate(pCtx, rqst)
			if err != nil {
				slog.Error("Не удалось обновить статус запроса в базе данных",
					slog.String("id", rqst.Id),
					"error", err,
				)
				continue
			}
			slog.Info("Запрос взят в фоновую обработку",
				slog.String("id", rqst.Id),
				slog.String("title", rqst.Title),
			)

			//ожидание в 1 сек для эмуляции какой-то деятельности
			select {
			case <-time.After(1 * time.Second):
				//обновление статуса заявки
				rqst.Status = "done"
				rqst.UpdatedAt = time.Now()

				err = repo.RecUpdate(pCtx, rqst)
				if err != nil {
					slog.Error("Не удалось обновить статус запроса в базе данных",
						slog.String("id", rqst.Id),
						"error", err,
					)
					continue
				}

				slog.Info("Запрос обработан",
					slog.String("id", rqst.Id),
					slog.String("title", rqst.Title),
				)

			case <-pCtx.Done():
				// если во время симуляции приложение экстренно завершают
				slog.Error("Приложение было закрыто при выполнении запроса",
					slog.String("title", rqst.Title),
					"error", err,
				)
				return pCtx.Err()
			}
		}
	}
}

// ф-ия инициализации воркеров
func WorkerInit(chnl <-chan *service.Request, wg *sync.WaitGroup, pCtx context.Context, repo service.RqstRepo) (err error) {
	// запуск трех воркеров
	for i := 0; i < 3; i++ {
		//увеличение счетчика заданий группы воркеров
		wg.Add(1)

		//запуск горутины Воркера
		go WorkerRun(chnl, wg, pCtx, repo)
	}

	return nil
}
