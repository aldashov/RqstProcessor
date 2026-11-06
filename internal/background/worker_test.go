package background

import (
	"context"
	"rqst_processor/internal/service"
	"sync"
	"testing"
	"time"
)

type workerMockRepository struct {
	LastStatus string // Переменная для фиксации изменений статуса в памяти
}

func (m *workerMockRepository) RecInsert(ctx context.Context, rqst *service.Request) error {
	return nil
}
func (m *workerMockRepository) RecDelete(ctx context.Context, rqst *service.Request) error {
	return nil
}
func (m *workerMockRepository) RecGetAll(ctx context.Context) ([]*service.Request, error) {
	return nil, nil
}
func (m *workerMockRepository) RecGetbyId(ctx context.Context, id string) (*service.Request, error) {
	return nil, nil
}

// Этот метод будет вызываться воркером. перехватываем статус задачи!
func (m *workerMockRepository) RecUpdate(ctx context.Context, rqst *service.Request) error {
	m.LastStatus = rqst.Status // Запоминаем, какой статус воркер пытается записать в БД
	return nil
}

// Тест сценария обработки задачи воркером
func TestWorkerRun_SuccessPath(t *testing.T) {
	// Инициализируем контексты и структуры
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	ch := make(chan *service.Request, 1)
	var wg sync.WaitGroup
	mockRepo := &workerMockRepository{}

	// Создаем тестовую задачу в статусе "new"
	rqst := &service.Request{
		Id:     "test-uuid",
		Title:  "Тестовая задача для воркера",
		Status: "new",
	}

	// Кладем задачу в канал
	ch <- rqst

	// Запускаем воркер асинхронно (в отдельной горутине)
	wg.Add(1)
	go func() {
		// Передаем наш умный mockRepo
		_ = WorkerRun(ch, &wg, appCtx, mockRepo)
	}()

	// пауза, чтобы воркер только успел перевести в processing
	time.Sleep(50 * time.Millisecond)

	// Проверяем промежуточный статус
	if mockRepo.LastStatus != "processing" {
		t.Errorf("Ожидался статус processing, но воркер записал: %s", mockRepo.LastStatus)
	}

	// пауза, чтобы воркер доделал задачу
	time.Sleep(1100 * time.Millisecond)

	// Инициируем Graceful Shutdown (закрываем канал и отменяем контекст)
	close(ch)
	appCancel()

	// Ждем штатного завершения горутины воркера
	wg.Wait()

	// Проверяем финальный статус
	if mockRepo.LastStatus != "done" {
		t.Errorf("Ожидался финальный статус done, но воркер записал: %s", mockRepo.LastStatus)
	}
}
