package service

import (
	"context"
	"time"
)

// тип Запрос
type Task struct {
	Id        string    `json:"id"`
	Title     string    `json:"title"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdat"`
	UpdatedAt time.Time `json:"updatedat"`
}

type TaskRepo interface {
	RecInsert(ctx context.Context, task *Task) error
	RecUpdate(ctx context.Context, task *Task) error
	RecDelete(ctx context.Context, task *Task) error
	RecGetAll(ctx context.Context) ([]*Task, error)
	RecGetbyId(ctx context.Context, id string) (*Task, error)
	Ping(ctx context.Context) error
}
