package web

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"rqst_processor/internal/service"
	"strings"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
)

type RqstHandler struct {
	repo     service.RqstRepo
	rqstChan chan<- *service.Request
}

type createRqstInput struct {
	Title string `json:"title"`
}

// конструктор структуры RqstHandler
func NewRqstHandler(repo service.RqstRepo, ch chan<- *service.Request) *RqstHandler {
	return &RqstHandler{
		repo:     repo,
		rqstChan: ch,
	}
}

// HandleCollection распределяет запросы на /rqsts
func (h *RqstHandler) HandleCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.CreateRqst(w, r) // функция создания
	case http.MethodGet:
		h.GetAllRqst(w, r) // функция получения списка
	default:
		w.WriteHeader(http.StatusMethodNotAllowed) // 405 если прислали что-то другое
	}
}

// HandleSingle распределяет запросы на /rqsts/{id}
func (h *RqstHandler) HandleSingle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.GetRqstbyId(w, r) // функция получения одной записи
	case http.MethodDelete:
		h.DeleteRqst(w, r) // функция удаления
	default:
		w.WriteHeader(http.StatusMethodNotAllowed) // 405
	}
}

// обработчик POST-запроса
func (h *RqstHandler) CreateRqst(w http.ResponseWriter, r *http.Request) {
	var input createRqstInput

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed) // 405
		return
	}

	//декодирование входящего JSON
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest) // Код 400
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON format"})
		return
	}

	//валидация данных
	if strings.TrimSpace(input.Title) == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest) // Код 400
		json.NewEncoder(w).Encode(map[string]string{"error": "Title is required"})
		return
	}

	//инициализация переменной Request
	rqst := &service.Request{
		Id:        uuid.New().String(),
		Title:     input.Title,
		Status:    "new",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	//сохранение запроса в БД
	if err := h.repo.RecInsert(r.Context(), rqst); err != nil {
		// КРИТИЧЕСКАЯ ТОЧКА: Полную ошибку пишем ТОЛЬКО в системный лог на сервере!
		slog.Error("Ошибка физической записи в PostgreSQL",
			slog.String("id", rqst.Id),
			"raw_error", err,
		)

		// Клиенту отдаем замаскированный, безопасный ответ и Код 500
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError) // Код 500
		json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	//передача задачи в канал воркерам для фоновой обработки
	h.rqstChan <- rqst

	//возврат ответа клиенту
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(rqst)
}

func (h *RqstHandler) DeleteRqst(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed) // 405
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/rqsts/")

	// 2. Валидация формата: если это не UUID, сразу отсекаем по Коду 400 Bad Request
	if _, err := uuid.Parse(idStr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid UUID format"})
		return
	}

	//инициализация переменной Request
	rqst := &service.Request{
		Id: idStr,
	}

	//удаление запроса из БД
	if err := h.repo.RecDelete(r.Context(), rqst); err != nil {
		// Если задача в статусе processing, RecDelete вернет ошибку RowsAffected == 0.
		// Отсекаем это как ошибку клиента (400 Bad Request)
		slog.Warn("Отклонено удаление записи", "id", idStr, "reason", err.Error())

		// Клиенту отдаем замаскированный, безопасный ответ и Код 500
		w.WriteHeader(http.StatusInternalServerError) // Код 500
		json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	slog.Info("Запись успешно удалена из СУБД", "id", idStr)

	//возврат ответа клиенту
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(rqst)
}

func (h *RqstHandler) GetAllRqst(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Вызываем метод репозитория для чтения массива структур
	// Передаем контекст запроса r.Context() для возможности отмены операции сетью
	rqsts, err := h.repo.RecGetAll(r.Context())
	if err != nil {
		// ТРЕБОВАНИЕ 2.6: Полный системный лог со сбоем пишем строго на сервере
		slog.Error("Не удалось вычитать список задач из СУБД", "error", err)

		// Клиенту отдаем безопасную маску и код 500
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	// 2. Если в базе пока нет ни одной задачи, PostgreSQL вернет пустой срез (nil)
	// Чтобы клиент получил красивый пустой JSON-массив [], а не null, делаем страховку
	if rqsts == nil {
		rqsts = make([]*service.Request, 0)
	}

	// 3. Успешный ответ: Код 200 OK и JSON-массив данных
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // Код 200
	json.NewEncoder(w).Encode(rqsts)
}

func (h *RqstHandler) GetRqstbyId(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Извлекаем ID из URL
	//idStr := r.PathValue("id")
	idStr := strings.TrimPrefix(r.URL.Path, "/tasks/")

	// Валидация формата: если это не UUID, сразу отсекаем по Коду 400 Bad Request
	if _, err := uuid.Parse(idStr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "Invalid UUID format"})
		return
	}

	// Вызываем оптимизированный метод репозитория (передаем контекст и строку ID)
	rqst, err := h.repo.RecGetbyId(r.Context(), idStr)
	if err != nil {
		// Проверяем: если запись просто не найдена в таблице
		if errors.Is(err, pgx.ErrNoRows) {
			w.WriteHeader(http.StatusNotFound) // Код 404
			json.NewEncoder(w).Encode(map[string]string{"error": "Task not found"})
			return
		}

		// Реальный технический сбой пишем строго в логи бэкенда
		slog.Error("Критический сбой СУБД при поиске по ID", "id", idStr, "error", err)

		w.WriteHeader(http.StatusInternalServerError) // Код 500
		json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	// Успешный ответ: Код 200 OK и JSON одной конкретной задачи
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(rqst)
}
