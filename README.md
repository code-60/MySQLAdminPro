# MySQLAdminPro

Локальный web-интерфейс для MySQL/MariaDB под macOS на Flask, собранный поверх твоих шаблонов из `templates/`.

## Что уже реализовано

- Авторизация по хосту/порту/логину/паролю MySQL
- Список баз данных
- Создание новой базы данных
- Список таблиц выбранной базы
- Просмотр строк таблицы (`SELECT * LIMIT N`)
- SQL Console по выбранной базе
- SQL-история запросов в рамках текущей сессии (последние 20)
- CRUD строк по `PRIMARY KEY`:
- Добавление строки
- Редактирование строки
- Удаление строки
- Пагинация в просмотре таблицы (`LIMIT/OFFSET`)
- Типизированные поля в Add/Edit форме:
- `datetime-local`, `date`, `time`
- `number`
- `boolean`/`enum`/`set` через `select`
- Выход (очистка сессии)

## Быстрый старт (режим разработки)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 app.py
```

После запуска открой `http://127.0.0.1:5001`.

## Запуск двойным кликом

Можно использовать `run_local.command` (открывает Terminal и запускает приложение):

```bash
chmod +x run_local.command
./run_local.command
```

## Сборка `.app` (без обязательного ручного запуска через терминал)

1. Установи зависимости и PyInstaller:

```bash
source .venv/bin/activate
pip install -r requirements.txt
pip install pyinstaller
```

2. Собери приложение:

```bash
chmod +x build_macos_app.sh
./build_macos_app.sh
```

3. Готовый пакет появится в `dist/MySQLAdminPro.app`.

При запуске `.app` стартует локальный сервер и откроется браузер.

## Структура проекта

- `app.py` — основной Flask backend
- `desktop_launcher.py` — entrypoint для desktop-сборки
- `templates/` — HTML-шаблоны интерфейса
- `build_macos_app.sh` — сборка `.app`
- `run_local.command` — локальный запуск

## Ограничения текущего MVP

- Имена БД/таблиц не должны быть пустыми и не могут содержать `/`
- SQL Console показывает превью результатов (до 500 строк)
- Edit/Delete строк работают только для таблиц, где есть `PRIMARY KEY`
- Для типа `set(...)` пока используется одиночный выбор (single-select)
