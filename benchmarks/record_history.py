"""Append custom benchmark results to the tracked documentation history."""

import json
import platform
from argparse import ArgumentParser
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl


def record_history(
    results_path: Path,
    history_path: Path,
    *,
    commit: str,
    repository: str,
    runner: str,
    timestamp: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    results = json.loads(results_path.read_text())
    if not isinstance(results, list) or not all(
        isinstance(result, dict) and {"name", "unit", "value"}.issubset(result)
        for result in results
    ):
        raise ValueError("benchmark results must be a list of name/unit/value objects")

    history: dict[str, Any] = (
        json.loads(history_path.read_text())
        if history_path.exists()
        else {"schema_version": 1, "suite": "ingestion", "entries": []}
    )
    entries = [
        entry for entry in history.get("entries", []) if entry.get("commit") != commit
    ]
    entries.append(
        {
            "commit": commit,
            "repository": repository,
            "timestamp": timestamp or datetime.now(UTC).isoformat(),
            "runner": runner,
            "python": platform.python_version(),
            "polars": pl.__version__,
            "benchmarks": results,
        }
    )
    history["entries"] = entries[-limit:]
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(json.dumps(history, indent=2) + "\n")
    return history


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("results", type=Path)
    parser.add_argument("history", type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--timestamp")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    try:
        record_history(
            args.results,
            args.history,
            commit=args.commit,
            repository=args.repository,
            runner=args.runner,
            timestamp=args.timestamp,
            limit=args.limit,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.exit(1, f"error: {exc}\n")


if __name__ == "__main__":
    main()
