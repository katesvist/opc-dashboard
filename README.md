# OPC UA Client Dashboard

Изолированный read-only визуальный стенд для `opc-ua-client-service`.

Стенд не является частью backend-клиента и может жить в отдельном репозитории. Он связан с клиентом через HTTP API:

- `GET /ready`
- `GET /connections`
- `GET /subscriptions`
- `GET /buffer/stats`
- `POST /read`
- `POST /browse`

В браузере авторизации нет. Если у клиента включен `management_token`, токен хранится только на стороне dashboard-сервера и передается в OPC UA client API как Bearer token.

## Быстрый запуск рядом с Docker-клиентом

Сначала подними сам OPC UA client из его репозитория:

```powershell
cd C:\Users\e.kanarskaya\Desktop\opc-ua-client-service-dev
docker compose -f .\docker-compose.server.yml up --build -d
```

Затем подними dashboard из этой папки:

```powershell
cd C:\Users\e.kanarskaya\Desktop\opc-ua-client-dashboard
docker compose up --build -d
```

Открой:

```text
http://127.0.0.1:8090
```

## Настройки

Через переменные окружения:

```text
OPC_CLIENT_BASE_URL=http://host.docker.internal:8080
OPC_CLIENT_TOKEN=secret-token
DASHBOARD_PORT=8090
```

Если dashboard запускается не в Docker, а локально:

```powershell
$env:OPC_CLIENT_BASE_URL = "http://127.0.0.1:8080"
$env:OPC_CLIENT_TOKEN = "secret-token"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8090
```

## Что показывает

- ready/not ready состояние клиента;
- состояние endpoint;
- active/inactive ноды;
- последние значения по нодам через `POST /read`;
- source timestamp и quality/status;
- buffer/dead-letter counters;
- простой browse адресного пространства.
