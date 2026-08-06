from __future__ import annotations

import asyncio
import io
from pathlib import Path

import pytest
from fastapi import HTTPException
from fastapi import UploadFile

from app.bindings_table import import_bindings_table
from app.main import bindings_template, export_bindings, import_bindings


def test_template_endpoint_returns_importable_xlsx() -> None:
    response = asyncio.run(bindings_template())
    content = Path(response.path).read_bytes()

    assert response.media_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    result = import_bindings_table("template.xlsx", content)
    assert result["accepted_rows"] == 0
    assert result["issues"] == []


def test_import_endpoint_normalizes_rows_without_saving() -> None:
    content = (
        "Node ID;Код параметра;Активна;Режим\n"
        "ns=3;s=DB.NUM;NUM;да;подписка\n"
    ).encode("utf-8")

    payload = asyncio.run(
        import_bindings(
            UploadFile(file=io.BytesIO(content), filename="bindings.csv")
        )
    )

    assert payload["accepted_rows"] == 1
    assert payload["rows"][0]["node_id"] == "ns=3;s=DB.NUM"
    assert payload["rows"][0]["acquisition_mode"] == "subscription"


def test_export_endpoint_returns_round_trip_xlsx() -> None:
    node = {
        "endpoint_id": "remote-opc-server",
        "node_id": "ns=3;s=DB.NUM",
        "parameter_code": "NUM",
        "enabled": True,
        "acquisition_mode": "subscription",
        "read_enabled": True,
        "write_enabled": False,
        "group_path": ["DB"],
        "tags": ["test"],
    }

    response = asyncio.run(export_bindings({"endpoint_id": "remote-opc-server", "nodes": [node]}))

    assert response.status_code == 200
    assert "opcua-bindings-remote-opc-server-" in response.headers["content-disposition"]
    result = import_bindings_table("export.xlsx", bytes(response.body))
    assert result["accepted_rows"] == 1
    assert result["rows"][0]["node_id"] == node["node_id"]
    assert result["rows"][0]["parameter_code"] == node["parameter_code"]


def test_export_endpoint_rejects_nodes_from_multiple_endpoints() -> None:
    nodes = [
        {"endpoint_id": "one", "node_id": "ns=2;s=One", "parameter_code": "ONE"},
        {"endpoint_id": "two", "node_id": "ns=2;s=Two", "parameter_code": "TWO"},
    ]

    with pytest.raises(HTTPException, match="одного выбранного endpoint") as error:
        asyncio.run(export_bindings({"endpoint_id": "one", "nodes": nodes}))

    assert error.value.status_code == 422


def test_import_endpoint_rejects_mixed_endpoint_table() -> None:
    content = (
        "Endpoint;Node ID;Код параметра\n"
        "one;ns=2;s=One;ONE\n"
        "two;ns=2;s=Two;TWO\n"
    ).encode("utf-8")

    with pytest.raises(HTTPException, match="несколько endpoint") as error:
        asyncio.run(
            import_bindings(
                UploadFile(file=io.BytesIO(content), filename="mixed.csv"),
                endpoint_id="one",
            )
        )

    assert error.value.status_code == 422


def test_import_endpoint_assigns_selected_endpoint_to_blank_table_column() -> None:
    content = (
        "Node ID;Код параметра\n"
        "ns=2;s=One;ONE\n"
    ).encode("utf-8")

    payload = asyncio.run(
        import_bindings(
            UploadFile(file=io.BytesIO(content), filename="bindings.csv"),
            endpoint_id="selected-endpoint",
        )
    )

    assert payload["endpoint_id"] == "selected-endpoint"
    assert payload["rows"][0]["endpoint_id"] == "selected-endpoint"
