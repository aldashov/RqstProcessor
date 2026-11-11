package background

import (
	"context"
	"sync"
	"task_processor/internal/service"
	"testing"
	"time"
)

type workerMockRepository struct {
	LastStatus string       // Переменная для фиксации изменений статуса в памяти
	mu         sync.RWMutex // Мьютекс для защиты от гонок данных
}

func (m *workerMockRepository) RecInsert(ctx context.Context, task *service.Task) error {
	return nil
}
func (m *workerMockRepository) RecDelete(ctx context.Context, task *service.Task) error {
	return nil
}
func (m *workerMockRepository) RecGetAll(ctx context.Context) ([]*service.Task, error) {
	return nil, nil
}
func (m *workerMockRepository) RecGetbyId(ctx context.Context, id string) (*service.Task, error) {
	return nil, nil
}
func (m *workerMockRepository) RecGetUnfinished(ctx context.Context) ([]*service.Task, error) {
	return nil, nil
}
func (m *workerMockRepository) Ping(ctx context.Context) error {
	return nil
}

// Этот метод будет вызываться воркером. перехватываем статус задачи! защищается мьютексом
func (m *workerMockRepository) RecUpdate(ctx context.Context, task *service.Task) error {
	m.mu.Lock()
	m.LastStatus = task.Status // Запоминаем, какой статус воркер пытается записать в БД
	m.mu.Unlock()
	return nil
}

// метод чтения статуса внутри теста
func (m *workerMockRepository) GetLastStatus() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.LastStatus
}

// Тест сценария обработки задачи воркером
func TestWorkerRun_SuccessPath(t *testing.T) {
	// Инициализируем контексты и структуры
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	ch := make(chan *service.Task, 1)
	var wg sync.WaitGroup
	mockRepo := &workerMockRepository{}

	// Создаем тестовую задачу в статусе "new"
	task := &service.Task{
		Id:     "test-uuid",
		Title:  "Тестовая задача для воркера",
		Status: "new",
	}

	// Кладем задачу в канал
	ch <- task

	// Запускаем воркер асинхронно (в отдельной горутине)
	wg.Add(1)
	// Передаем наш умный mockRepo
	go func() {
		// Передаем быструю задержку 100мс для прохождения теста без долгих ожиданий
		_ = WorkerRunWithDuration(ch, &wg, appCtx, mockRepo, 100*time.Millisecond)
	}()

	// пауза, чтобы воркер только успел перевести в processing
	time.Sleep(20 * time.Millisecond)

	// Проверяем промежуточный статус
	if currentStatus := mockRepo.GetLastStatus(); currentStatus != "processing" {
		t.Errorf("Ожидался статус processing, но воркер записал: %s", currentStatus)
	}

	// пауза, чтобы воркер доделал задачу
	time.Sleep(150 * time.Millisecond)

	// Инициируем Graceful Shutdown (закрываем канал и отменяем контекст)
	close(ch)
	appCancel()

	// Ждем штатного завершения горутины воркера
	wg.Wait()

	// Проверяем финальный статус
	if finalStatus := mockRepo.GetLastStatus(); finalStatus != "done" {
		t.Errorf("Ожидался финальный статус done, но воркер записал: %s", finalStatus)
	}
}
