package web

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"task_processor/internal/service"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
)

type TaskHandler struct {
	repo     service.TaskRepo
	taskChan chan<- *service.Task
}

type createTaskInput struct {
	Title string `json:"title"`
}

// конструктор структуры TaskHandler
func NewTaskHandler(repo service.TaskRepo, ch chan<- *service.Task) *TaskHandler {
	return &TaskHandler{
		repo:     repo,
		taskChan: ch,
	}
}

// HandleCollection распределяет запросы на /tasks
func (h *TaskHandler) HandleCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.CreateTask(w, r) // функция создания
	case http.MethodGet:
		h.GetAllTask(w, r) // функция получения списка
	default:
		w.WriteHeader(http.StatusMethodNotAllowed) // 405 если прислали что-то другое
	}
}

// HandleSingle распределяет запросы на /tasks/{id}
func (h *TaskHandler) HandleSingle(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.GetTaskbyId(w, r) // функция получения одной записи
	case http.MethodDelete:
		h.DeleteTask(w, r) // функция удаления
	default:
		w.WriteHeader(http.StatusMethodNotAllowed) // 405
	}
}

// обработчик POST-запроса
func (h *TaskHandler) CreateTask(w http.ResponseWriter, r *http.Request) {
	var input createTaskInput

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed) // 405
		return
	}

	// декодирование входящего JSON
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest) // Код 400
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON format"})
		return
	}

	// валидация данных
	if strings.TrimSpace(input.Title) == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest) // Код 400
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Title is required"})
		return
	}

	// инициализация переменной Task
	task := &service.Task{
		Id:        uuid.New().String(),
		Title:     input.Title,
		Status:    "new",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// сохранение запроса в БД
	if err := h.repo.RecInsert(r.Context(), task); err != nil {
		slog.Error("Ошибка физической записи в PostgreSQL",
			slog.String("id", task.Id),
			"raw_error", err,
		)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError) // Код 500
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	// Снимаем копию ДО передачи указателя в канал воркерам, чтобы исключить Data Race
	respTask := *task

	// передача задачи в канал воркерам для фоновой обработки
	h.taskChan <- task

	// возврат ответа клиенту
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(respTask)
}

func (h *TaskHandler) DeleteTask(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed) // 405
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/tasks/")

	// Валидация формата: если это не UUID, сразу отсекаем по Коду 400 Bad Request
	if _, err := uuid.Parse(idStr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid UUID format"})
		return
	}

	task := &service.Task{
		Id: idStr,
	}

	// удаление запроса из БД
	if err := h.repo.RecDelete(r.Context(), task); err != nil {
		slog.Warn("Отклонено удаление записи", "id", idStr, "reason", err.Error())

		if strings.Contains(err.Error(), "There are no deleted records") {
			w.WriteHeader(http.StatusBadRequest) // Код 400 (задача не найдена или в статусе processing)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Task cannot be deleted while processing or not found"})
			return
		}

		w.WriteHeader(http.StatusInternalServerError) // Код 500 при техническом сбое БД
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	slog.Info("Запись успешно удалена из СУБД", "id", idStr)

	// возврат ответа клиенту
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"result": "OK"})
}

func (h *TaskHandler) GetAllTask(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tasks, err := h.repo.RecGetAll(r.Context())
	if err != nil {
		slog.Error("Не удалось вычитать список задач из СУБД", "error", err)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	if tasks == nil {
		tasks = make([]*service.Task, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(tasks)
}

func (h *TaskHandler) GetTaskbyId(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	idStr := strings.TrimPrefix(r.URL.Path, "/tasks/")

	if _, err := uuid.Parse(idStr); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Invalid UUID format"})
		return
	}

	task, err := h.repo.RecGetbyId(r.Context(), idStr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			w.WriteHeader(http.StatusNotFound) // Код 404
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "Task not found"})
			return
		}

		slog.Error("Критический сбой СУБД при поиске по ID", "id", idStr, "error", err)

		w.WriteHeader(http.StatusInternalServerError) // Код 500
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "Internal server error"})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(task)
}

func (h *TaskHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := h.repo.Ping(r.Context()); err != nil {
		slog.Error("Ошибка при проверке health: БД не доступна", "error", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy"})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}
