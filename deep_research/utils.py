from __future__ import annotations

import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\x00", " ")).strip().lower()


def tokenize(text: str) -> list[str]:
    return [token for token in re.findall(r"[\u4e00-\u9fffA-Za-z0-9]+", text.lower()) if len(token) >= 2]


def score_text(text: str, keywords: list[str]) -> float:
    haystack = normalize_text(text)
    score = 0.0
    for keyword in keywords:
        if keyword.lower() in haystack:
            score += 1.0
    return score


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def append_jsonl(path: Path, item: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")


def log_trace(path: Path, agent: str, step: str, status: str, **kwargs: Any) -> None:
    payload = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "agent": agent,
        "step": step,
        "status": status,
    }
    payload.update(kwargs)
    append_jsonl(path, payload)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))
