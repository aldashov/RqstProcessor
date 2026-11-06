package web

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"rqst_processor/internal/service"
	"testing"
)

type successMockRepository struct {
	SavedTask *service.Request // Сюда запишется результат генерации хендлера
}

func (m *successMockRepository) RecUpdate(ctx context.Context, rqst *service.Request) error {
	return nil
}
func (m *successMockRepository) RecDelete(ctx context.Context, rqst *service.Request) error {
	return nil
}
func (m *successMockRepository) RecGetAll(ctx context.Context) ([]*service.Request, error) {
	return nil, nil
}
func (m *successMockRepository) RecGetbyId(ctx context.Context, id string) (*service.Request, error) {
	return nil, nil
}

// Перехватываем вызов вставки в БД
func (m *successMockRepository) RecInsert(ctx context.Context, rqst *service.Request) error {
	m.SavedTask = rqst // Запоминаем объект, собранный хендлером
	return nil
}

// Тест успешного создания новой записи (Передача только Title)
func TestCreateTask_SuccessPath(t *testing.T) {
	// Создаем буферизованный канал на 1 элемент, чтобы хендлер не заблокировался при отправке
	ch := make(chan *service.Request, 1)
	mockRepo := &successMockRepository{}
	handler := NewRqstHandler(mockRepo, ch)

	// Симулируем клиента — отправляем СТРОГО ОДИН TITLE в JSON
	jsonBody := []byte(`{"title": "Изучить интерфейсы в Go"}`)
	req, _ := http.NewRequest("POST", "/tasks", bytes.NewBuffer(jsonBody))

	rr := httptest.NewRecorder()

	// Вызываем метод создания задачи
	handler.CreateRqst(rr, req)

	// Проверяем правильность HTTP-статуса
	if rr.Code != http.StatusCreated { // Должен быть 201 Created
		t.Errorf("expected status 201, got %d", rr.Code)
	}

	// Проверяем, что хендлер сгенерировал системные поля перед записью в БД
	if mockRepo.SavedTask == nil {
		t.Fatal("Хендлер не вызвал метод RecInsert репозитория")
	}

	// Проверяем, что Title дошел без искажений
	if mockRepo.SavedTask.Title != "Изучить интерфейсы в Go" {
		t.Errorf("expected title 'Изучить интерфейсы в Go', got '%s'", mockRepo.SavedTask.Title)
	}

	// Проверяем автоматическую генерацию ID (длина UUID строки должна быть 36 символов)
	if len(mockRepo.SavedTask.Id) != 36 {
		t.Errorf("Хендлер не сгенерировал валидный ID, длина: %d", len(mockRepo.SavedTask.Id))
	}

	// Проверяем правильность начального статуса
	if mockRepo.SavedTask.Status != "new" {
		t.Errorf("expected initial status 'new', got '%s'", mockRepo.SavedTask.Status)
	}

	// Проверяем, что задача улетела в канал воркерам
	if len(ch) != 1 {
		t.Error("Задача не была поставлена в очередь (канал воркеров пуст)")
	}
}
