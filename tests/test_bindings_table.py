from __future__ import annotations

import io

import pytest
from openpyxl import load_workbook

from app.bindings_table import BindingTableError, export_bindings_xlsx, import_bindings_table


def test_export_import_round_trip_preserves_binding_fields() -> None:
    content = export_bindings_xlsx(
        [
            {
                "endpoint_id": "remote-opc-server",
                "node_id": 'ns=3;s="DB  FOR  TEST"."Temperature"',
                "parameter_code": "temperature.outdoor",
                "parameter_name": "Температура наружного воздуха",
                "enabled": False,
                "acquisition_mode": "polling",
                "read_enabled": True,
                "write_enabled": False,
                "sampling_interval_ms": 1500,
                "polling_interval_seconds": 2.5,
                "group_path": ["DB FOR TEST", "Climate"],
                "browse_name": "3:Temperature",
                "display_name": "Temperature",
                "expected_type": "float",
                "value_shape": "scalar",
                "unit": "°C",
                "tags": ["climate", "test"],
            }
        ]
    )

    result = import_bindings_table("bindings.xlsx", content)

    assert result["accepted_rows"] == 1
    assert result["issues"] == []
    row = result["rows"][0]
    assert row["source_row"] == 5
    assert row["node_id"] == 'ns=3;s="DB  FOR  TEST"."Temperature"'
    assert row["enabled"] is False
    assert row["acquisition_mode"] == "polling"
    assert row["polling_interval_seconds"] == 2.5
    assert row["group_path"] == ["DB FOR TEST", "Climate"]
    assert row["tags"] == ["climate", "test"]


def test_csv_accepts_human_headers_russian_values_and_decimal_comma() -> None:
    content = (
        "Источник;Идентификатор ноды;Код параметра;Активно;Режим;Подписка, мс;Опрос, с;Группа;Теги\n"
        'remote-opc-server;"ns=3;s=""DB"".""NUM""";NUM;да;подписка;1000;2,5;DB / Values;one, two\n'
    ).encode("utf-8")

    result = import_bindings_table("bindings.csv", content)

    assert result["accepted_rows"] == 1
    assert result["issues"] == []
    row = result["rows"][0]
    assert row["endpoint_id"] == "remote-opc-server"
    assert row["node_id"] == 'ns=3;s="DB"."NUM"'
    assert row["enabled"] is True
    assert row["acquisition_mode"] == "subscription"
    assert row["polling_interval_seconds"] == 2.5
    assert row["group_path"] == ["DB", "Values"]
    assert row["tags"] == ["one", "two"]


def test_import_reports_row_errors_without_rejecting_valid_rows() -> None:
    content = (
        "Node ID;Код параметра;Режим;Подписка, мс\n"
        "ns=2;s=Good;GOOD;subscription;1000\n"
        "ns=2;s=Bad;;something;10,5\n"
    ).encode("utf-8")

    result = import_bindings_table("bindings.csv", content)

    assert result["accepted_rows"] == 1
    assert result["rows"][0]["parameter_code"] == "GOOD"
    assert {issue["field"] for issue in result["issues"]} == {
        "parameter_code",
        "acquisition_mode",
        "sampling_interval_ms",
    }


def test_xlsx_formulas_are_rejected() -> None:
    content = export_bindings_xlsx([])
    workbook = load_workbook(io.BytesIO(content))
    sheet = workbook["Привязки"]
    sheet["B5"] = "=1+1"
    sheet["C5"] = "PARAM"
    output = io.BytesIO()
    workbook.save(output)

    with pytest.raises(BindingTableError, match="Формулы не поддерживаются"):
        import_bindings_table("bindings.xlsx", output.getvalue())


def test_unknown_file_type_is_rejected() -> None:
    with pytest.raises(BindingTableError, match="Поддерживаются"):
        import_bindings_table("bindings.xls", b"not-an-xls")
