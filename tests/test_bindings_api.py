from __future__ import annotations

import asyncio
import io
from pathlib import Path

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

    response = asyncio.run(export_bindings({"nodes": [node]}))

    assert response.status_code == 200
    result = import_bindings_table("export.xlsx", bytes(response.body))
    assert result["accepted_rows"] == 1
    assert result["rows"][0]["node_id"] == node["node_id"]
    assert result["rows"][0]["parameter_code"] == node["parameter_code"]
