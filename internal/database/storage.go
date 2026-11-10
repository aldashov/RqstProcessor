package database

import (
	"context"
	"fmt"
	"task_processor/internal/service"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

// создание новой структуры
func NewRepo(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// добавление записи в т-цу БД
func (rd *Repository) RecInsert(pCtx context.Context, task *service.Task) (err error) {
	//создание локального контекста для пула
	l_ctx, l_ctx_cncl := context.WithTimeout(pCtx, 2*time.Second)
	defer l_ctx_cncl()

	_, err = rd.pool.Exec(l_ctx, "insert into request (id, title, status, createdat, updatedat) values ($1, $2, $3, $4, $5)", task.Id, task.Title, task.Status, task.CreatedAt, task.UpdatedAt)
	if err != nil {
		return err
	}

	return err
}

// обновление записи в т-це БД
func (rd *Repository) RecUpdate(pCtx context.Context, task *service.Task) (err error) {
	//создание локального контекста для пула
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 2*time.Second)
	defer lCtxCncl()

	_, err = rd.pool.Exec(lCtx, "update request set status = $1, updatedat = $2 where id = $3", task.Status, task.UpdatedAt, task.Id)
	if err != nil {
		return err
	}

	return err
}

// удаление записи из т-цы БД
func (rd *Repository) RecDelete(pCtx context.Context, task *service.Task) (err error) {
	//создание локального контекста для пула
	lCtx, lCtx_cncl := context.WithTimeout(pCtx, 2*time.Second)
	defer lCtx_cncl()

	cTag, err := rd.pool.Exec(lCtx, "delete from request where id = $1 and status != $2", task.Id, "processing")
	if err != nil {
		return err
	}

	if cTag.RowsAffected() == 0 {

		return fmt.Errorf("There are no deleted records with key %v", task.Title)
	}

	return err
}

// получение списка всех записей из БД
func (rd *Repository) RecGetAll(pCtx context.Context) ([]*service.Task, error) {
	// Создаем локальный контекст с таймаутом на чтение всей таблицы
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 5*time.Second)
	defer lCtxCncl()

	rows, err := rd.pool.Query(lCtx, "select id, title, status, createdat, updatedat from request")
	if err != nil {
		return nil, err
	}
	// Гарантируем закрытие дескриптора rows для предотвращения утечки пула соединений
	defer rows.Close()

	var task []*service.Task

	// Цикл сканирования строк базы данных
	for rows.Next() {
		t := &service.Task{}
		// Сканируем поля строго в том порядке, в котором они указаны в SELECT
		err := rows.Scan(&t.Id, &t.Title, &t.Status, &t.CreatedAt, &t.UpdatedAt)
		if err != nil {
			return nil, err
		}
		task = append(task, t) // Добавляем заполненный объект в массив
	}

	// Проверяем, не прервалось ли чтение из-за ошибок внутри цикла rows.Next()
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return task, nil
}

// получение уникальной записи из БД по ключу id
// RecGetbyId ищет ровно одну задачу по её ID.
// Если задача не найдена, возвращает ошибку pgx.ErrNoRows.
func (rd *Repository) RecGetbyId(pCtx context.Context, id string) (*service.Task, error) {
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 3*time.Second)
	defer lCtxCncl()

	t := &service.Task{}

	// Используем QueryRow вместо Query. Метод Scan вызывается цепочкой (chaining).
	err := rd.pool.QueryRow(lCtx,
		"select id, title, status, createdat, updatedat from request where id = $1",
		id,
	).Scan(&t.Id, &t.Title, &t.Status, &t.CreatedAt, &t.UpdatedAt)

	if err != nil {
		// Если ошибка — это отсутствие записи (pgx.ErrNoRows),
		// мы просто пробрасываем её наверх, хендлер превратит её в 404.
		return nil, err
	}

	return t, nil
}

func (rd *Repository) Ping(pCtx context.Context) error {
	lCtx, cancel := context.WithTimeout(pCtx, 2*time.Second)
	defer cancel()

	return rd.pool.Ping(lCtx)
}
