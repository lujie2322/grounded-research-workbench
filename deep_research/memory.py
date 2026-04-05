from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import ResearchTask


class WorkflowMemory:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"runs": [], "task_patterns": {}, "report_highlights": []}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {"runs": [], "task_patterns": {}, "report_highlights": []}

    def save(self, data: dict[str, Any]) -> None:
        self.path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def update_after_run(
        self,
        task: ResearchTask,
        summary: dict[str, Any],
    ) -> dict[str, Any]:
        data = self.load()
        runs = data.get("runs", [])
        runs.append(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "task": asdict(task),
                "summary": summary,
            }
        )
        data["runs"] = runs[-12:]

        patterns = data.get("task_patterns", {})
        for keyword in task.keywords:
            patterns[keyword] = patterns.get(keyword, 0) + 1
        data["task_patterns"] = dict(sorted(patterns.items(), key=lambda item: item[1], reverse=True)[:50])

        highlights = data.get("report_highlights", [])
        highlights.extend(summary.get("highlights", []))
        data["report_highlights"] = highlights[-30:]
        self.save(data)
        return data
