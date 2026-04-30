from __future__ import annotations

import asyncio
import json
import os
import socket
from base64 import b64encode
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


BASE_DIR = Path(__file__).parent
STATIC_DIR = BASE_DIR / "static"


@dataclass(frozen=True)
class ClientSettings:
    base_url: str
    token: str | None
    params_service_base_url: str
    timeout_seconds: float = 300.0
    rabbitmq_management_url: str | None = None
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_vhost: str = "/"
    rabbitmq_queue: str = "validator.in.q"

    @classmethod
    def from_env(cls) -> ClientSettings:
        return cls(
            base_url=os.getenv("OPC_CLIENT_BASE_URL", "http://127.0.0.1:8080").rstrip("/"),
            token=os.getenv("OPC_CLIENT_TOKEN") or None,
            params_service_base_url=os.getenv("PARAMS_SERVICE_BASE_URL", "http://127.0.0.1:8000").rstrip("/"),
            timeout_seconds=float(os.getenv("OPC_CLIENT_TIMEOUT_SECONDS", "300")),
            rabbitmq_management_url=(
                os.getenv("RABBITMQ_MANAGEMENT_URL", "http://host.docker.internal:15672").rstrip("/")
            ),
            rabbitmq_user=os.getenv("RABBITMQ_USER", "guest"),
            rabbitmq_password=os.getenv("RABBITMQ_PASSWORD", "guest"),
            rabbitmq_vhost=os.getenv("RABBITMQ_VHOST", "/"),
            rabbitmq_queue=os.getenv("RABBITMQ_QUEUE", "validator.in.q"),
        )


class OpcClientApi:
    def __init__(self, settings: ClientSettings) -> None:
        self.settings = settings

    async def get(self, path: str, *, tolerate_error: bool = False) -> dict[str, Any] | list[Any] | str:
        return await asyncio.to_thread(self._request, "GET", path, None, tolerate_error)

    async def get_result(self, path: str) -> tuple[int, dict[str, Any] | list[Any] | str]:
        return await asyncio.to_thread(self._request_result, "GET", path, None)

    async def post(self, path: str, body: dict[str, Any]) -> dict[str, Any] | list[Any] | str:
        return await asyncio.to_thread(self._request, "POST", path, body, False)

    async def put(self, path: str, body: dict[str, Any]) -> dict[str, Any] | list[Any] | str:
        return await asyncio.to_thread(self._request, "PUT", path, body, False)

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None,
        tolerate_error: bool,
    ) -> dict[str, Any] | list[Any] | str:
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(
            f"{self.settings.base_url}{path}",
            data=payload,
            method=method,
            headers=self._headers(has_body=body is not None),
        )
        try:
            with urlopen(request, timeout=self.settings.timeout_seconds) as response:
                return self._decode_response(response.read(), response.headers.get("content-type", ""))
        except HTTPError as exc:
            decoded = self._decode_response(exc.read(), exc.headers.get("content-type", ""))
            if tolerate_error:
                return decoded
            raise HTTPException(status_code=exc.code, detail=decoded) from exc
        except URLError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"OPC UA client API is unavailable: {exc.reason}",
            ) from exc
        except (TimeoutError, socket.timeout) as exc:
            raise HTTPException(
                status_code=504,
                detail=f"OPC UA client API timed out after {self.settings.timeout_seconds:g}s.",
            ) from exc

    def _request_result(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None,
    ) -> tuple[int, dict[str, Any] | list[Any] | str]:
        payload = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(
            f"{self.settings.base_url}{path}",
            data=payload,
            method=method,
            headers=self._headers(has_body=body is not None),
        )
        try:
            with urlopen(request, timeout=self.settings.timeout_seconds) as response:
                return response.status, self._decode_response(response.read(), response.headers.get("content-type", ""))
        except HTTPError as exc:
            decoded = self._decode_response(exc.read(), exc.headers.get("content-type", ""))
            return exc.code, decoded
        except URLError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"OPC UA client API is unavailable: {exc.reason}",
            ) from exc
        except (TimeoutError, socket.timeout) as exc:
            raise HTTPException(
                status_code=504,
                detail=f"OPC UA client API timed out after {self.settings.timeout_seconds:g}s.",
            ) from exc

    def _headers(self, *, has_body: bool) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if has_body:
            headers["Content-Type"] = "application/json"
        if self.settings.token:
            headers["Authorization"] = f"Bearer {self.settings.token}"
        return headers

    def _decode_response(self, raw: bytes, content_type: str = "") -> dict[str, Any] | list[Any] | str:
        if not raw:
            return {}
        text = raw.decode("utf-8")
        if "application/json" not in content_type:
            return text
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text


class RabbitMqApi:
    def __init__(self, settings: ClientSettings) -> None:
        self.settings = settings

    async def queue_stats(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._queue_stats)

    def _queue_stats(self) -> dict[str, Any]:
        if not self.settings.rabbitmq_management_url:
            return {"available": False, "error": "RabbitMQ management URL is not configured."}

        vhost = self._quote_path_part(self.settings.rabbitmq_vhost)
        queue = self._quote_path_part(self.settings.rabbitmq_queue)
        request = Request(
            f"{self.settings.rabbitmq_management_url}/api/queues/{vhost}/{queue}",
            method="GET",
            headers={
                "Accept": "application/json",
                "Authorization": f"Basic {self._basic_token()}",
            },
        )
        try:
            with urlopen(request, timeout=self.settings.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
                return {
                    "available": True,
                    "queue": payload.get("name", self.settings.rabbitmq_queue),
                    "messages": payload.get("messages", 0),
                    "messages_ready": payload.get("messages_ready", 0),
                    "messages_unacknowledged": payload.get("messages_unacknowledged", 0),
                    "consumers": payload.get("consumers", 0),
                }
        except HTTPError as exc:
            return {
                "available": False,
                "queue": self.settings.rabbitmq_queue,
                "error": f"RabbitMQ management returned HTTP {exc.code}.",
            }
        except URLError as exc:
            return {
                "available": False,
                "queue": self.settings.rabbitmq_queue,
                "error": f"RabbitMQ management is unavailable: {exc.reason}",
            }

    def _basic_token(self) -> str:
        raw = f"{self.settings.rabbitmq_user}:{self.settings.rabbitmq_password}".encode("utf-8")
        return b64encode(raw).decode("ascii")

    def _quote_path_part(self, value: str) -> str:
        from urllib.parse import quote

        return quote(value, safe="")


class ParamsServiceApi:
    def __init__(self, settings: ClientSettings) -> None:
        self.settings = settings

    async def dictionary_snapshot(self) -> dict[str, Any]:
        params, datatypes, units = await asyncio.gather(
            self._fetch_paged("/api/v1/dict/params"),
            self._get("/api/v1/dict/datatypes"),
            self._get("/api/v1/dict/units"),
        )
        datatype_by_id = {item["id"]: item for item in datatypes if isinstance(item, dict)}
        unit_by_id = {item["id"]: item for item in units if isinstance(item, dict)}
        enriched_params = []
        for item in params:
            if not isinstance(item, dict):
                continue
            datatype = datatype_by_id.get(item.get("datatype_id"))
            unit = unit_by_id.get(item.get("unit_id"))
            enriched_params.append(
                {
                    **item,
                    "datatype": datatype,
                    "unit": unit,
                    "datatype_name": datatype.get("name") if datatype else None,
                    "unit_name": unit.get("name") if unit else None,
                    "unit_symbol": unit.get("symbol") if unit else None,
                }
            )
        return {
            "base_url": self.settings.params_service_base_url,
            "params": enriched_params,
            "datatypes": datatypes if isinstance(datatypes, list) else [],
            "units": units if isinstance(units, list) else [],
        }

    async def _fetch_paged(self, path: str, *, page_size: int = 1000) -> list[Any]:
        items: list[Any] = []
        offset = 0
        while True:
            separator = "&" if "?" in path else "?"
            page = await self._get(f"{path}{separator}limit={page_size}&offset={offset}")
            if not isinstance(page, list):
                return items
            items.extend(page)
            if len(page) < page_size:
                return items
            offset += page_size

    async def _get(self, path: str) -> dict[str, Any] | list[Any] | str:
        return await asyncio.to_thread(self._request, path)

    def _request(self, path: str) -> dict[str, Any] | list[Any] | str:
        request = Request(
            f"{self.settings.params_service_base_url}{path}",
            method="GET",
            headers={"Accept": "application/json"},
        )
        try:
            with urlopen(request, timeout=self.settings.timeout_seconds) as response:
                return self._decode_response(response.read(), response.headers.get("content-type", ""))
        except HTTPError as exc:
            decoded = self._decode_response(exc.read(), exc.headers.get("content-type", ""))
            raise HTTPException(status_code=exc.code, detail=decoded) from exc
        except URLError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Params service API is unavailable: {exc.reason}",
            ) from exc

    def _decode_response(self, raw: bytes, content_type: str = "") -> dict[str, Any] | list[Any] | str:
        if not raw:
            return {}
        text = raw.decode("utf-8")
        if "application/json" not in content_type:
            return text
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text


class ClientApiRequest(BaseModel):
    operation: str
    endpoint_id: str | None = None
    node_id: str | None = None
    value: Any = None
    max_depth: int = 1
    include_variables: bool = True
    include_objects: bool = True


settings = ClientSettings.from_env()
client_api = OpcClientApi(settings)
rabbitmq_api = RabbitMqApi(settings)
params_api = ParamsServiceApi(settings)
app = FastAPI(
    title="OPC UA Client Dashboard",
    version="0.1.0",
)
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/snapshot")
async def snapshot() -> dict[str, Any]:
    health_payload, ready_payload, connections, subscriptions, buffer_stats, rabbitmq_stats = await asyncio.gather(
        _safe_client_get("/health", fallback={}),
        _safe_client_get("/ready", fallback={}),
        _safe_client_get("/connections", fallback=[]),
        _safe_client_get("/subscriptions", fallback=[]),
        _safe_client_get("/buffer/stats", fallback={}),
        rabbitmq_api.queue_stats(),
    )

    if not isinstance(connections, list):
        connections = []
    if not isinstance(subscriptions, list):
        subscriptions = []
    if not isinstance(buffer_stats, dict):
        buffer_stats = {}

    nodes = await _build_nodes_snapshot(subscriptions)
    return {
        "updated_at": datetime.now(UTC).isoformat(),
        "client": {
            "base_url": settings.base_url,
            "params_service_base_url": settings.params_service_base_url,
            "timeout_seconds": settings.timeout_seconds,
        },
        "healthy": (
            isinstance(health_payload, dict)
            and str(health_payload.get("status", "")).lower() == "ok"
        ),
        "health": health_payload,
        "ready": bool(ready_payload.get("ready")) if isinstance(ready_payload, dict) else False,
        "readiness": ready_payload,
        "connections": connections,
        "subscriptions": subscriptions,
        "nodes": nodes,
        "buffer": buffer_stats,
        "rabbitmq": rabbitmq_stats,
    }


async def _safe_client_get(path: str, fallback: Any) -> Any:
    try:
        return await client_api.get(path, tolerate_error=True)
    except HTTPException as exc:
        return {"error": exc.detail, "available": False} if isinstance(fallback, dict) else fallback


@app.get("/api/operations")
async def operations() -> list[dict[str, Any]]:
    return [
        {"id": "health", "title": "Health", "method": "GET", "path": "/health", "group": "technical"},
        {"id": "ready", "title": "Ready", "method": "GET", "path": "/ready", "group": "technical"},
        {"id": "metrics", "title": "Metrics", "method": "GET", "path": "/metrics", "group": "technical"},
        {"id": "connections", "title": "Connections", "method": "GET", "path": "/connections", "group": "technical"},
        {"id": "subscriptions", "title": "Subscriptions", "method": "GET", "path": "/subscriptions", "group": "functional"},
        {"id": "buffer_stats", "title": "Buffer stats", "method": "GET", "path": "/buffer/stats", "group": "technical"},
        {"id": "dead_letter", "title": "Dead letter", "method": "GET", "path": "/dead-letter", "group": "technical"},
        {"id": "browse", "title": "Browse", "method": "POST", "path": "/browse", "group": "functional", "needs": ["endpoint_id"]},
        {"id": "read", "title": "Read", "method": "POST", "path": "/read", "group": "functional", "needs": ["endpoint_id", "node_id"]},
        {"id": "write", "title": "Write", "method": "POST", "path": "/write", "group": "functional", "needs": ["endpoint_id", "node_id", "value"]},
        {
            "id": "reconnect",
            "title": "Reconnect",
            "method": "POST",
            "path": "/connections/{endpoint_id}/reconnect",
            "group": "functional",
            "needs": ["endpoint_id"],
        },
    ]


@app.get("/api/config/nodes")
async def config_nodes() -> dict[str, Any]:
    try:
        response = await client_api.get("/config/nodes")
        return {"nodes": response if isinstance(response, list) else []}
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=_unwrap_detail(exc.detail)) from exc


@app.put("/api/config/nodes")
async def replace_config_nodes(payload: dict[str, Any]) -> dict[str, Any] | list[Any] | str:
    nodes = payload.get("nodes")
    if not isinstance(nodes, list):
        raise HTTPException(status_code=422, detail="nodes list is required")
    try:
        return await client_api.put("/config/nodes", {"nodes": nodes})
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=_unwrap_detail(exc.detail)) from exc


@app.get("/api/dictionary")
async def dictionary() -> dict[str, Any]:
    return await params_api.dictionary_snapshot()


@app.post("/api/client/request")
async def client_request(payload: ClientApiRequest) -> dict[str, Any]:
    try:
        response = await _execute_client_operation(payload)
        return {
            "ok": True,
            "operation": payload.operation,
            "response": response,
        }
    except HTTPException as exc:
        return {
            "ok": False,
            "operation": payload.operation,
            "status_code": exc.status_code,
            "response": exc.detail,
        }
    except Exception as exc:
        return {
            "ok": False,
            "operation": payload.operation,
            "status_code": 500,
            "response": str(exc),
        }


@app.get("/api/browse")
async def browse(
    endpoint_id: str = Query(..., min_length=1),
    node_id: str | None = None,
    max_depth: int = Query(1, ge=0, le=5),
    include_variables: bool = True,
    include_objects: bool = True,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "endpoint_id": endpoint_id,
        "max_depth": max_depth,
        "include_variables": include_variables,
        "include_objects": include_objects,
    }
    if node_id:
        body["node_id"] = node_id
    try:
        response = await client_api.post("/browse", body)
    except HTTPException as exc:
        raise HTTPException(status_code=exc.status_code, detail=_unwrap_detail(exc.detail)) from exc
    return {
        "endpoint_id": endpoint_id,
        "node_id": node_id,
        "max_depth": max_depth,
        "items": response if isinstance(response, list) else [],
    }


async def _execute_client_operation(payload: ClientApiRequest) -> dict[str, Any] | list[Any] | str:
    operation = payload.operation
    if operation == "health":
        return await client_api.get("/health")
    if operation == "ready":
        status_code, response = await client_api.get_result("/ready")
        if status_code >= 400:
            raise HTTPException(status_code=status_code, detail=_unwrap_detail(response))
        return response
    if operation == "metrics":
        return await client_api.get("/metrics")
    if operation == "connections":
        return await client_api.get("/connections")
    if operation == "subscriptions":
        return await client_api.get("/subscriptions")
    if operation == "buffer_stats":
        return await client_api.get("/buffer/stats")
    if operation == "dead_letter":
        return await client_api.get("/dead-letter")
    if operation == "browse":
        endpoint_id = _require_field(payload.endpoint_id, "endpoint_id")
        body: dict[str, Any] = {
            "endpoint_id": endpoint_id,
            "max_depth": payload.max_depth,
            "include_variables": payload.include_variables,
            "include_objects": payload.include_objects,
        }
        if payload.node_id:
            body["node_id"] = payload.node_id
        return await client_api.post("/browse", body)
    if operation == "read":
        return await client_api.post(
            "/read",
            {
                "endpoint_id": _require_field(payload.endpoint_id, "endpoint_id"),
                "node_id": _require_field(payload.node_id, "node_id"),
            },
        )
    if operation == "write":
        if payload.value is None:
            raise HTTPException(status_code=422, detail="value is required")
        return await client_api.post(
            "/write",
            {
                "endpoint_id": _require_field(payload.endpoint_id, "endpoint_id"),
                "node_id": _require_field(payload.node_id, "node_id"),
                "value": payload.value,
            },
        )
    if operation == "reconnect":
        endpoint_id = _require_field(payload.endpoint_id, "endpoint_id")
        return await client_api.post(f"/connections/{endpoint_id}/reconnect", {})
    raise HTTPException(status_code=404, detail=f"Unsupported operation: {operation}")


def _require_field(value: str | None, field_name: str) -> str:
    if not value:
        raise HTTPException(status_code=422, detail=f"{field_name} is required")
    return value


def _unwrap_detail(detail: Any) -> Any:
    if isinstance(detail, dict) and set(detail) == {"detail"}:
        return detail["detail"]
    return detail


async def _build_nodes_snapshot(subscriptions: list[Any]) -> list[dict[str, Any]]:
    async def enrich(item: dict[str, Any]) -> dict[str, Any]:
        read_result = None
        read_error = None
        if item.get("active") and item.get("endpoint_id") and item.get("node_id"):
            try:
                response = await client_api.post(
                    "/read",
                    {
                        "endpoint_id": item["endpoint_id"],
                        "node_id": item["node_id"],
                    },
                )
                read_result = response if isinstance(response, dict) else None
            except HTTPException as exc:
                read_error = str(exc.detail)
        return {
            "endpoint_id": item.get("endpoint_id"),
            "node_id": item.get("node_id"),
            "parameter_code": item.get("parameter_code"),
            "acquisition_mode": item.get("acquisition_mode"),
            "status": item,
            "read": read_result,
            "read_error": read_error,
        }

    typed_subscriptions = [item for item in subscriptions if isinstance(item, dict)]
    return await asyncio.gather(*(enrich(item) for item in typed_subscriptions))


@app.get("/api/client-link")
async def client_link(path: str = "/health") -> dict[str, str]:
    query = urlencode({"path": path})
    return {
        "base_url": settings.base_url,
        "path": path,
        "hint": f"Dashboard proxies browser requests to the configured OPC UA client API. Query: {query}",
    }
