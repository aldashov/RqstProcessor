package service

import (
	"context"
	"time"
)

// тип Запрос
type Request struct {
	Id        string    `json:"id"`
	Title     string    `json:"title"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdat"`
	UpdatedAt time.Time `json:"updatedat"`
}

type RqstRepo interface {
	RecInsert(ctx context.Context, rqst *Request) error
	RecUpdate(ctx context.Context, rqst *Request) error
	RecDelete(ctx context.Context, rqst *Request) error
	RecGetAll(ctx context.Context) ([]*Request, error)
	RecGetbyId(ctx context.Context, id string) (*Request, error)
}
