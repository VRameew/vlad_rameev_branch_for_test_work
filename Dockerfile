# Фаза 1: Базовый образ с PostgreSQL
FROM postgres:13-alpine AS postgres

# Фаза 2: Базовый образ с FastAPI и Strawberry
FROM python:3.9-slim-buster AS app

RUN apt update && apt install -y gcc cmake libpq-dev python-dev

# Установка зависимостей для FastAPI и Strawberry
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода приложения
COPY . .

# Установка переменных окружения
ENV DATABASE_URL postgresql://user:password@postgres:5432/db

# Запуск приложения
CMD ["python", "./main.py"]
