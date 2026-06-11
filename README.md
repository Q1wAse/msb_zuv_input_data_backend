# msb_zuv_input_data_backend

API для ввода данных "Материально-стоимостной баланс ЖУВ".

## Требования

- Python 3.8+
- PostgreSQL 14+

## Разовая настройка

### 1. Виртуальное окружение

Из папки бэкенда:

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

### 2. База данных

Два варианта — выбери один.

**Вариант А — через Docker (не нужен локальный PostgreSQL)**

```bash
docker compose up -d
```

Создаст и запустит контейнер с PostgreSQL 15, база `msb_zuv_input_data_tables` на порту 5432.
Остановить: `docker compose down`. Данные сохраняются в именованном volume.

**Вариант Б — локальный PostgreSQL**

Если PostgreSQL уже установлен и запущен на порту 5432, Docker не нужен.
Создать базу вручную:

```bash
psql -U postgres -h 127.0.0.1 -p 5432 -c "CREATE DATABASE msb_zuv_input_data_tables;"
```

### 3. Заливка данных

При получении SQL-файлов от разработчика залить их в таком порядке:

```bash
# 1. Структура таблиц
psql -U postgres -h 127.0.0.1 -p 5432 -d msb_zuv_input_data_tables -f <файл_со_структурой>.sql

# 2. Справочные данные
psql -U postgres -h 127.0.0.1 -p 5432 -d msb_zuv_input_data_tables -f <файл_со_словарями>.sql
```

Ошибки вида "роль IS_KAO не существует" — не критичны, это артефакт продакшн-дампа.

## Запуск

```bash
bash dev.sh
```

Swagger UI: http://localhost:5000/

## Переменные окружения

| Переменная   | По умолчанию                                                             | Описание                |
|--------------|--------------------------------------------------------------------------|-------------------------|
| DATABASE_URL | postgresql://postgres:postgres@127.0.0.1:5432/msb_zuv_input_data_tables | Строка подключения к БД |
| MSB_DATA_DIR | <папка бэкенда> | Путь к папке с файлами (в продакшне: /opt/foresight/msb_zuv_input_data_backend) |
