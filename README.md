# Analytics Transfer App
## Требования

- Python 3.13+
- PostgreSQL (создайте БД analytics_db)
- S3-совместимое хранилище (MinIO или аналог)

## Установка и конфигурация
### Общие шаги
1) Клонируйте репозиторий:
```
git clone https://github.com/alevch10/s3topostgres.git
cd s3topostgres
```

2) Установите зависимости с Poetry:
poetry install

3) Скопируйте .env:
```
cp .env.example .env
```

Отредактируйте .env

4) Инициализируйте миграции Alembic:
```
poetry run alembic init app/migrations
```
5) Сгенерируйте первую миграцию:
```
poetry run alembic revision --autogenerate -m "Initial create web and mp tables"
```

6) Примените миграции:
```
poetry run alembic upgrade head
```

## Запуск для Linux/macOS

1) Запустите сервер:
```
poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
2) Запустите процесс переноса (POST /start):
```
curl -X POST "http://localhost:8000/start?prefix=your/folder/&table_name=web"
```
3) Проверьте статус (GET /status):
```
curl "http://localhost:8000/status"
```

## Запуск для Windows

1) Установите Poetry (если не стоит): Скачайте с https://python-poetry.org/docs/#installation
2) Запустите сервер (в PowerShell или CMD):
```
poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
3) Запустите процесс (используйте curl или Postman):
```
curl -X POST "http://localhost:8000/start?prefix=your/folder/&table_name=web"
```
4) Статус:
```
curl "http://localhost:8000/status"
```

## Тестирование

- Документация API: http://localhost:8000/docs
- Журнал хранится в journal.json (локально).

## Остановка

- Ctrl+C для сервера.
- Для background-процесса: kill <pid> (из ответа /start).

Если ошибки - проверьте логи в app.log. Для dev: debug=true в .env.

## Структура приложения: 
```
s3topostgres/
├── pyproject.toml
├── alembic.ini
├── .env.example
├── README.md
├── app/
│   ├── __init__.py
│   ├── main.py  # FastAPI app
│   ├── logger.py  # Новый файл
│   ├── config.py  # Settings с Pydantic
│   ├── s3_client.py  # Boto3 клиент
│   ├── models.py  # SQLAlchemy модели
│   ├── database.py  # Подключение к БД
│   ├── journal.py  # Локальный журнал (JSON файл)
│   ├── processor.py  # Логика обработки (background process)
│   ├── migrations/  # Alembic миграции (автогенерированные)
│   │   ├── env.py
│   │   ├── script.py.mako
│   │   └── versions/
│   └── api/
│       └── endpoints.py  # API роуты
└── alembic/  # Генерируется Alembic
```

## Логирование
- Логи пишутся в файл `app.log` (или значение из `log_file` в .env) и в консоль.
- Уровень: INFO по умолчанию, DEBUG если `debug=true` в .env.
- Пример просмотра логов:
  - Linux/macOS: `tail -f app.log`
  - Windows: `Get-Content app.log -Wait` (в PowerShell)