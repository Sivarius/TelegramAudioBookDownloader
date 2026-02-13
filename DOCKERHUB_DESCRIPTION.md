# Docker Hub Description

Repository: `amatsukine/telegram-audiobook-downloader`

## Short Description (EN, <=100 chars)
Telegram user-API audiobook channel downloader with web UI, MTProxy, and optional SFTP upload.

## Короткое описание (RU, <=100 chars)
Загрузчик аудиокниг из Telegram-каналов через user API с веб UI, MTProxy и SFTP.

---

## Full Description (EN)
`TelegramAudioBookDownloader` is a web-based service that connects to Telegram via **user API (Telethon)**, monitors audiobook channels, downloads audio chapters, and can optionally upload files to SFTP.

### Key features
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

---

## Полное описание (RU)
`TelegramAudioBookDownloader` - веб-сервис, который подключается к Telegram через **user API (Telethon)**, отслеживает каналы с аудиокнигами, скачивает главы и при необходимости загружает их на SFTP.

### Основные возможности
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
