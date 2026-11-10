.PHONY: up down stop logs ps test test-local tidy

# Поднятие основного приложения и основной БД в фоне
up:
	docker compose up --build -d

# Мягкая остановка основного приложения (Graceful Shutdown)
stop:
	docker compose stop

# Полная остановка основного приложения с удалением контейнеров и сетей
down:
	docker compose down -v

# Просмотр логов основного приложения в реальном времени
logs:
	docker compose logs -f app

# Просмотр статуса контейнеров основного окружения
ps:
	docker compose ps

# Запуск тестов в изолированном Docker-приложении (требование наставника)
test:
	docker compose -f docker-compose.test.yml up --build --abort-on-container-exit
	docker compose -f docker-compose.test.yml down -v

# Быстрый запуск тестов локально без Docker (требует установленного Go)
test-local:
	go test -v -race ./...

# Очистка и обновление зависимостей Go
tidy:
	go mod tidy
