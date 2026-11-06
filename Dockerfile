# Шаг 1: Сборочный контейнер (Builder)
FROM golang:1.27-alpine AS builder

WORKDIR /app

# Копируем манифесты зависимостей
COPY go.mod go.sum ./
RUN go mod download

# Копируем исходный код проекта
COPY . .

# Собираем статический бинарник без отладочных символов (оптимизация по весу)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o server cmd/server/main.go

# Шаг 2: Финальный продакшн-контейнер
FROM alpine:3.19

WORKDIR /app

# Копируем только скомпилированный бинарник из первого контейнера
COPY --from=builder /app/server .

# Открываем порт нашего веб-сервера
EXPOSE 8081

# Инструкция запуска
CMD ["./server"]
