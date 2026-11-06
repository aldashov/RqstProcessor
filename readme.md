# Request Processor Service

Асинхронный процессор задач на Go со слоем персистентности в PostgreSQL и отказоустойчивым пулом воркеров.

## Технологический стек
- Go 1.23+ ( layered architecture )
- PostgreSQL 15 ( pgxpool driver )
- Docker / Docker Compose ( Multi-stage build )
- Structured logging ( log/slog )


## Быстрый запуск инфраструктуры
Для запуска в Docker всё работает автоматически. Для локального запуска без Docker необходимо указать ваш локальный пароль СУБД в функции db_init()»

1. Сделайте копию шаблона окружения: `cp .env.example .env`
2. Запустите сборку и оркестрацию всей среды командой:
   ```bash
   docker compose up --build
   ```

## Примеры API запросов (Порт 8081)
- **Создание задачи (POST):** `curl -X POST http://localhost:8081/rqsts -H "Content-Type: application/json" -d "{\"title\":\"Новая задача\"}"`
- **Получение списка (GET):** `curl -X GET http://localhost:8081/rqsts`
- **Удаление по ID (DELETE):** `curl -X DELETE http://localhost:8081/rqsts/uuid_строка`
- **Healthcheck (GET):** `curl -X GET http://localhost:8081/health`
