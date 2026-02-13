# TeleGaParser

Python-скрипт для отслеживания публичного Telegram-канала через **user API** (Telethon) и автоматического скачивания новых аудио-файлов.

Проект создан при помощи **Visual Studio Code + Codex + Context7**.

## Docker Hub Overview (EN/RU)
Repository: `amatsukine/telegram-audiobook-downloader`

Short description (EN, <=100 chars):
`Telegram user-API audiobook channel downloader with web UI, MTProxy, and optional SFTP upload.`

Короткое описание (RU, <=100 chars):
`Загрузчик аудиокниг из Telegram-каналов через user API с веб UI, MTProxy и SFTP.`

Full description (EN):
`TelegramAudioBookDownloader` is a web-based service that connects to Telegram via **user API (Telethon)**, monitors audiobook channels, downloads audio chapters, and can optionally upload files to SFTP.

Key features:
- Telegram authorization with saved session (`remember me`)
- Channel preview with chapter indexing
- Parallel downloads with live per-file progress (SSE)
- Resume-safe downloads via `.part` files and integrity checks
- MTProxy support via a single proxy link field
- Channel history with periodic checks and auto-download rules
- Optional SFTP upload with remote duplicate check and hash/size verification
- SQLite state tracking per channel (resume from last downloaded chapter)
- Docker/Docker Compose ready

Use this image if you need a self-hosted service to continuously collect audiobook chapters from Telegram channels and optionally mirror them to SFTP.

Полное описание (RU):
`TelegramAudioBookDownloader` - веб-сервис, который подключается к Telegram через **user API (Telethon)**, отслеживает каналы с аудиокнигами, скачивает главы и при необходимости загружает их на SFTP.

Основные возможности:
- Авторизация Telegram с сохранением сессии (`запомнить меня`)
- Предпросмотр канала с нумерацией глав
- Параллельные загрузки и live-прогресс по каждому файлу (SSE)
- Безопасная докачка через `.part` и проверки целостности
- Поддержка MTProxy через одно поле-ссылку
- История каналов, периодические проверки и правила автозагрузки
- Опциональная загрузка на SFTP с проверкой дублей и сверкой hash/размера
- Хранение состояния в SQLite по каждому каналу (продолжение с последней главы)
- Готовность к запуску в Docker/Docker Compose

Используйте этот образ, если нужен self-hosted сервис для постоянного сбора глав аудиокниг из Telegram-каналов и опционального зеркалирования на SFTP.

## Что делает
- Подключается как пользователь Telegram (не bot API)
- Следит за указанным публичным каналом
- Скачивает новые аудио из новых постов
- При старте может догонять последние N сообщений
- Не скачивает уже обработанные сообщения (SQLite-реестр)
- Сохраняет рабочие настройки в `bot_data.sqlite3`
- При каждом запуске спрашивает путь для сохранения аудио
- Автоматически создаёт подпапку по названию канала внутри указанного пути
- Хранит прогресс по каждому `channel_id` и продолжает со следующего файла

## Требования
- Python 3.10+
- Telegram API credentials (`api_id`, `api_hash`) с https://my.telegram.org
- Для высокой скорости загрузки рекомендуется `cryptg` (устанавливается из `requirements.txt`)
- Для загрузки на SFTP используется `paramiko` (устанавливается из `requirements.txt`)

## Быстрый старт
1. Запустить веб-интерфейс:
   ```bash
   start_web_ui.bat
   ```
2. Если `.venv` ещё нет, скрипт сам запустит установку зависимостей.
3. Если `.env` отсутствует, он автоматически создастся из `.env.example`.
4. В веб-форме заполнить:
   - `API_ID`
   - `API_HASH`
   - `PHONE` (в международном формате)
   - `CHANNEL_ID` (например `@my_books_channel` или `-1001234567890`)

При первом запуске Telethon попросит код подтверждения из Telegram через форму в веб-интерфейсе.

Альтернативно можно запустить установку вручную:
```bash
install_dependencies.bat
```

## Docker Compose
1. Заполнить `.env` (если файла нет, скопировать из `.env.example`).
2. Прод-образ в Docker Hub: `amatsukine/telegram-audiobook-downloader:latest`.
3. Для dev-режима (монтируется весь проект):
   ```bash
   docker compose up -d --build
   ```
4. Для prod-режима (только данные, без монтирования исходников):
   ```bash
   docker compose -f docker-compose.prod.yml up -d --build
   ```
   Каталог `./data` и файл БД будут созданы автоматически.
5. Открыть:
   - `http://127.0.0.1:8080`

Остановка:
```bash
docker compose down
# или для prod:
docker compose -f docker-compose.prod.yml down
```

## Веб-интерфейс
- Запуск: `start_web_ui.bat`
- При старте автоматически открывается страница `http://127.0.0.1:8080`
- В форме указываются:
  - `API_ID`, `API_HASH`, `PHONE`
  - `CHANNEL_ID`
  - путь сохранения аудио
- Кнопка `Авторизоваться`:
  - запоминает учетную запись через Telethon session
  - при необходимости запрашивает код из Telegram/2FA пароль
- Кнопка `Обновить предпросмотр`:
  - показывает найденные в канале аудио справа с нумерацией
- Поля `С индекса` / `По индекс`:
  - позволяют выбрать диапазон аудио для начального скачивания
  - чекбокс `Скачать новые` автоматически подбирает диапазон новых глав и блокирует ручной ввод
- `Потоков загрузки`:
  - количество одновременных скачиваний (по умолчанию `3`)
  - при `FloodWait` бот автоматически снижает параллельность
- `Использовать mtproxy`:
  - включает одно поле для строки вида `server=...&port=...&secret=...`
  - бот сам разбирает строку на host/port/secret
- `Загружать на SFTP`:
  - после скачивания локальной главы загружает её в удалённый каталог
  - перед загрузкой сверяется с файлами на SFTP и не грузит повторы
  - после загрузки проверяет совпадение размера и SHA-256 хэша
  - есть отдельная кнопка `Проверить SFTP`
- `История каналов`:
  - показывает ранее использованные `channel_id` с названием канала
  - отображает статус наличия новых аудио для быстрого переключения
  - содержит чекбоксы:
    - `Проверять наличие новых глав`
    - `Включить автозагрузку`
    - `Включить автозагрузку на SFTP`
    - `Очистка локального после SFTP`
  - сервис проверяет каналы по расписанию (по умолчанию раз в 2 часа)
  - фоновая периодическая проверка выключена по умолчанию и включается чекбоксом `Периодически проверять новые главы`
  - если в этот момент идёт активное скачивание, фоновая проверка откладывается (чтобы не блокировать Telethon session)
- `Запомнить меня`:
  - сохраняет сессию Telethon и не требует повторной авторизации
- Кнопка `Стоп сервер`:
  - безопасно останавливает загрузчик и затем веб-сервер
- Чекбокс `Дебаг`:
  - открывает встроенную консоль внизу страницы
  - показывает live-логи приложения для диагностики ошибок
- Кнопка `Начать скачивание`:
  - проверяет доступ к каналу
  - запускает скачивание в фоне
  - статус скачивания обновляется автоматически на странице (SSE)
  - отображается прогресс по каждому файлу при параллельной загрузке
  - при обрыве/аварийной остановке недокачанные файлы автоматически перекачиваются при следующем запуске
  - загрузка ведётся через `.part` и завершается атомарной заменой файла после проверки размера
  - при включённом SFTP параллельно показывает счётчики загрузки на SFTP

## Переменные окружения
- `API_ID` - числовой ID приложения
- `API_HASH` - hash приложения
- `PHONE` - номер пользователя
- `CHANNEL_ID` - username или числовой ID канала
- `DOWNLOAD_DIR` - директория для файлов (по умолчанию `downloads`)
- `SESSION_NAME` - имя файла сессии Telethon (по умолчанию `user_session`)
- `STARTUP_SCAN_LIMIT` - сколько последних сообщений проверять при старте (по умолчанию `200`, `0` отключает догон)
- `DOWNLOAD_CONCURRENCY` - сколько файлов скачивать параллельно (по умолчанию `3`)
- `USE_MTPROXY` - `1` или `0`
- `MTPROXY_LINK` - строка `server=...&port=...&secret=...`
- `USE_SFTP` - `1` или `0`
- `SFTP_HOST` - адрес SFTP сервера
- `SFTP_PORT` - порт SFTP (по умолчанию `22`)
- `SFTP_USERNAME` - логин
- `SFTP_PASSWORD` - пароль
- `SFTP_REMOTE_DIR` - базовый удаленный каталог для книг
- `CLEANUP_LOCAL_AFTER_SFTP` - `1` или `0`, удалять локальный файл после подтверждённой SFTP загрузки
- `AUTO_CHECK_INTERVAL_SECONDS` - интервал проверки каналов для автозадач (по умолчанию `7200`)

## Примечания
- Скрипт рассчитан на публичный канал, куда нельзя добавить бота.
- Для приватных каналов пользователь должен иметь доступ к каналу в своем аккаунте.
- База `bot_data.sqlite3` хранит состояние канала, включая последний скачанный файл.
