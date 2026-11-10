package web

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"task_processor/internal/service"
	"testing"
)

type successMockRepository struct {
	SavedTask   *service.Task // Сюда запишется результат генерации хендлера
	DeleteError error         // Сюда можно передать ошибку для симуляции сбоя удаления
}

func (m *successMockRepository) RecUpdate(ctx context.Context, task *service.Task) error {
	return nil
}
func (m *successMockRepository) RecGetAll(ctx context.Context) ([]*service.Task, error) {
	return nil, nil
}
func (m *successMockRepository) RecGetbyId(ctx context.Context, id string) (*service.Task, error) {
	return nil, nil
}
func (m *successMockRepository) Ping(ctx context.Context) error {
	return nil
}

// Перехватываем вызов вставки в БД
func (m *successMockRepository) RecInsert(ctx context.Context, task *service.Task) error {
	m.SavedTask = task // Запоминаем объект, собранный хендлером
	return nil
}

// Перехватываем вызов удаления
func (m *successMockRepository) RecDelete(ctx context.Context, task *service.Task) error {
	// Если в тесте была задана ошибка (например, симуляция статуса processing), возвращаем её
	if m.DeleteError != nil {
		return m.DeleteError
	}
	return nil
}

// Тест успешного создания новой записи (Передача только Title)
func TestCreateTask_SuccessPath(t *testing.T) {
	// Создаем буферизованный канал на 1 элемент, чтобы хендлер не заблокировался при отправке
	ch := make(chan *service.Task, 1)
	mockRepo := &successMockRepository{}
	handler := NewTaskHandler(mockRepo, ch)

	// Симулируем клиента — отправляем СТРОГО ОДИН TITLE в JSON
	jsonBody := []byte(`{"title": "Изучить интерфейсы в Go"}`)
	req, _ := http.NewRequest("POST", "/tasks", bytes.NewBuffer(jsonBody))

	rr := httptest.NewRecorder()

	// Вызываем метод создания задачи
	handler.CreateTask(rr, req)

	// Проверяем правильность HTTP-статуса
	if rr.Code != http.StatusCreated { // Должен быть 201 Created
		t.Errorf("Ожидался статус 201, поступил %d", rr.Code)
	}

	// Проверяем, что хендлер сгенерировал системные поля перед записью в БД
	if mockRepo.SavedTask == nil {
		t.Fatal("Хендлер не вызвал метод RecInsert репозитория")
	}

	// Проверяем, что Title дошел без искажений
	if mockRepo.SavedTask.Title != "Изучить интерфейсы в Go" {
		t.Errorf("Ожидалось значение 'Изучить интерфейсы в Go' в поле title, поступило '%s'", mockRepo.SavedTask.Title)
	}

	// Проверяем автоматическую генерацию ID (длина UUID строки должна быть 36 символов)
	if len(mockRepo.SavedTask.Id) != 36 {
		t.Errorf("Хендлер не сгенерировал валидный ID, длина: %d", len(mockRepo.SavedTask.Id))
	}

	// Проверяем правильность начального статуса
	if mockRepo.SavedTask.Status != "new" {
		t.Errorf("Ожидался начальный статус 'new', поступил '%s'", mockRepo.SavedTask.Status)
	}

	// Проверяем, что задача улетела в канал воркерам
	if len(ch) != 1 {
		t.Error("Задача не была поставлена в очередь (канал воркеров пуст)")
	}
}

// Тест 2: Валидация входных данных (Пустой Title должен возвращать 400)
func TestCreateTask_ValidationEmptyTitle(t *testing.T) {
	ch := make(chan *service.Task, 1)
	mockRepo := &successMockRepository{}
	handler := NewTaskHandler(mockRepo, ch)

	// Передаем пустую строку в title
	jsonBody := []byte(`{"title": "   "}`)
	req, _ := http.NewRequest("POST", "/tasks", bytes.NewBuffer(jsonBody))
	rr := httptest.NewRecorder()

	handler.CreateTask(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Ожидался статус 400 для незаполненного title, поступило %d", rr.Code)
	}
	if mockRepo.SavedTask != nil {
		t.Error("Репозиторий не должен вызываться при ошибочных входных данных")
	}
	if len(ch) != 0 {
		t.Error("Задача с незаполненным title не должна попадать в канал воркеров")
	}
}

// Тест 3: Проверка запрета удаления задачи, если она находится в статусе processing
func TestDeleteTask_ForbiddenWhileProcessing(t *testing.T) {
	ch := make(chan *service.Task, 1)
	mockRepo := &successMockRepository{}

	// Настраиваем мок так, будто БД отклонила удаление (RowsAffected == 0 из-за статуса processing)
	mockRepo.DeleteError = errors.New("Нельзя удалить обрабатываемую в данный момент запись")
	handler := NewTaskHandler(mockRepo, ch)

	// Формируем запрос на удаление с валидным UUID
	req, _ := http.NewRequest("DELETE", "/tasks/00000000-0000-0000-0000-000000000000", nil)
	rr := httptest.NewRecorder()

	handler.DeleteTask(rr, req)

	// Хендлер при ошибке удаления должен вернуть 500 (или 400 в зависимости от логики маскирования)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("Ожидался статус 500, когда удаляется обрабатываемая в данный момент задача, поступил %d", rr.Code)
	}
}
