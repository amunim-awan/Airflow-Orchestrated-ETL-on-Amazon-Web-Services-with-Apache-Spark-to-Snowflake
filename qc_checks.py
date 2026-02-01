"""Data Quality checks.

These are simple, fast checks you can run in Airflow and/or inside Spark jobs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class QCResult:
    name: str
    passed: bool
    detail: str = ""


def require_min_rows(row_count: int, minimum: int, name: str = "min_rows") -> QCResult:
    passed = row_count >= minimum
    detail = f"row_count={row_count}, minimum={minimum}"
    return QCResult(name=name, passed=passed, detail=detail)


def require_columns_present(columns: Iterable[str], required: Iterable[str], name: str = "required_columns") -> QCResult:
    cols = set(c.lower() for c in columns)
    missing = [c for c in required if c.lower() not in cols]
    passed = len(missing) == 0
    detail = "missing=" + ",".join(missing) if missing else "ok"
    return QCResult(name=name, passed=passed, detail=detail)


def fail_if_any_failed(results: list[QCResult]) -> None:
    failed = [r for r in results if not r.passed]
    if failed:
        msg = "QC failed: " + " | ".join(f"{r.name} ({r.detail})" for r in failed)
        raise RuntimeError(msg)
