package database

import (
	"context"
	"errors"
	"fmt"
	"task_processor/internal/service"
	"time"

	"github.com/jackc/pgx/v5"
	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

// создание новой структуры
func NewRepo(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// добавление записи в т-цу БД
func (rd *Repository) RecInsert(pCtx context.Context, task *service.Task) (err error) {
	//создание локального контекста для пула
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 2*time.Second)
	defer lCtxCncl()

	return rd.db.WithContext(lCtx).Create(task).Error
}

// обновление записи в т-це БД
func (rd *Repository) RecUpdate(pCtx context.Context, task *service.Task) (err error) {
	//создание локального контекста для пула
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 2*time.Second)
	defer lCtxCncl()

	return rd.db.WithContext(lCtx).
		Model(&service.Task{}).
		Where("id = ?", task.Id).
		Select("Status", "UpdatedAt").
		Updates(task).Error
}

// удаление записи из т-цы БД
func (rd *Repository) RecDelete(pCtx context.Context, task *service.Task) (err error) {
	//создание локального контекста для пула
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 2*time.Second)
	defer lCtxCncl()

	res := rd.db.WithContext(lCtx).
		Where("id = ? AND status != ?", task.Id, "processing").
		Delete(&service.Task{})
	if res.Error != nil {
		return res.Error
	}

	if res.RowsAffected == 0 {
		return fmt.Errorf("There are no deleted records with key %v", task.Title)
	}

	return nil
}

// получение списка всех записей из БД
func (rd *Repository) RecGetAll(pCtx context.Context) ([]*service.Task, error) {
	// Создаем локальный контекст с таймаутом на чтение всей таблицы
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 5*time.Second)
	defer lCtxCncl()

	var tasks []*service.Task
	err := rd.db.WithContext(lCtx).Find(&tasks).Error
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

// получение уникальной записи из БД по ключу id
// RecGetbyId ищет ровно одну задачу по её ID.
// Если задача не найдена, возвращает ошибку pgx.ErrNoRows.
func (rd *Repository) RecGetbyId(pCtx context.Context, id string) (*service.Task, error) {
	lCtx, lCtxCncl := context.WithTimeout(pCtx, 3*time.Second)
	defer lCtxCncl()

	var t service.Task
	err := rd.db.WithContext(lCtx).Where("id = ?", id).First(&t).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, pgx.ErrNoRows
		}
		return nil, err
	}

	return &t, nil
}

func (rd *Repository) Ping(pCtx context.Context) error {
	lCtx, cancel := context.WithTimeout(pCtx, 2*time.Second)
	defer cancel()

	sqlDB, err := rd.db.DB()
	if err != nil {
		return err
	}

	return sqlDB.PingContext(lCtx)
}
