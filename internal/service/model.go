package service

import (
	"context"
	"time"
)

// тип Запрос
type Task struct {
	Id        string    `json:"id" gorm:"column:id;primaryKey"`
	Title     string    `json:"title" gorm:"column:title"`
	Status    string    `json:"status" gorm:"column:status"`
	CreatedAt time.Time `json:"createdat" gorm:"column:createdat;autoCreateTime:false"`
	UpdatedAt time.Time `json:"updatedat" gorm:"column:updatedat;autoUpdateTime:false"`
}

func (Task) TableName() string {
	return "request"
}

type TaskRepo interface {
	RecInsert(ctx context.Context, task *Task) error
	RecUpdate(ctx context.Context, task *Task) error
	RecDelete(ctx context.Context, task *Task) error
	RecGetAll(ctx context.Context) ([]*Task, error)
	RecGetbyId(ctx context.Context, id string) (*Task, error)
	Ping(ctx context.Context) error
}
