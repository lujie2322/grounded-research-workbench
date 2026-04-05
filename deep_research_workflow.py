#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from deep_research import DeepResearchWorkflow, ResearchTask, WorkflowConfig


DEFAULT_CONFIG = {
    "project_name": "行业深度研究工作流",
    "outdir": "/Users/jie/Desktop/editor/output/deep_research",
    "memory_path": "/Users/jie/Desktop/editor/output/deep_research/workflow_memory.json",
    "trace_path": "/Users/jie/Desktop/editor/output/deep_research/workflow_trace.jsonl",
    "literature_csv": "/Users/jie/Desktop/editor/output/grounded_monitor_integration/literature_table.csv",
    "theme_memory_path": "/Users/jie/Desktop/editor/output/grounded_monitor_integration/theme_memory.json",
    "compact_context_path": "/Users/jie/Desktop/editor/output/grounded_monitor_integration/compact_context.md",
    "local_text_paths": [
        "/Users/jie/Desktop/editor/output/grounded_monitor_integration",
        "/Users/jie/Desktop/editor/output/grounded_monitor_smoke",
    ],
    "structured_data_paths": [],
    "local_pdf_paths": [
        "/Users/jie/Desktop/editor/paper_fetch_output/pdfs",
    ],
    "workflow": {
        "enable_searcher": True,
        "enable_collector": True,
        "enable_analyst": True,
        "enable_aggregator": True,
        "max_literature_items": 12,
        "max_text_items": 10,
        "max_structured_items": 8,
        "max_context_chars": 24000,
        "chart_top_n": 8,
        "max_news_items": 8,
        "max_policy_items": 8,
        "max_community_items": 8,
    },
    "llm": {
        "enabled": False,
        "provider": "openai_compatible",
        "model": "",
        "api_url": "",
        "api_key_env": "DEEP_RESEARCH_API_KEY",
        "temperature": 0.2,
    },
    "connectors": {
        "finance": {
            "preferred_providers": ["baostock", "yahoo_finance", "akshare"],
            "baostock_start_date": "2024-01-01",
            "yahoo_history_period": "6mo",
            "enable_symbol_financials": False
        },
        "news": {
            "provider": "google_news_rss",
            "max_items": 8
        },
        "policy": {
            "provider": "gov_cn_policy_library",
            "max_items": 8
        },
        "community": {
            "providers": ["eastmoney_guba", "stocktwits"],
            "max_items": 8,
            "per_symbol_limit": 3
        }
    },
}


def merge_dict(base: dict, patch: dict) -> dict:
    merged = json.loads(json.dumps(base, ensure_ascii=False))
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def build_task(args: argparse.Namespace) -> ResearchTask:
    return ResearchTask(
        task=args.task,
        mode=args.mode,
        market_scope=[item.strip() for item in args.market_scope.split(",") if item.strip()],
        symbols=[item.strip() for item in args.symbols.split(",") if item.strip()],
        metrics=[item.strip() for item in args.metrics.split(",") if item.strip()],
        keywords=[item.strip() for item in args.keywords.split(",") if item.strip()],
        report_style=args.report_style,
        output_name=args.output_name.strip(),
    )


def load_config(path: Path | None) -> WorkflowConfig:
    user_config = {}
    if path:
        user_config = json.loads(path.read_text(encoding="utf-8"))
    merged = merge_dict(DEFAULT_CONFIG, user_config)
    return WorkflowConfig(**merged)


def main() -> int:
    parser = argparse.ArgumentParser(description="End-to-end multi-agent deep research workflow")
    parser.add_argument("--config", type=Path, default=None, help="Optional JSON config path")
    parser.add_argument("--task", type=str, required=True, help="Research task or report request")
    parser.add_argument("--mode", type=str, default="report", choices=["report", "analysis"])
    parser.add_argument("--symbols", type=str, default="", help="Comma separated symbols, e.g. 600519,00700,TSLA")
    parser.add_argument("--metrics", type=str, default="", help="Comma separated metrics")
    parser.add_argument("--keywords", type=str, default="", help="Comma separated search keywords")
    parser.add_argument("--market-scope", type=str, default="", help="Comma separated market scopes")
    parser.add_argument("--report-style", type=str, default="professional")
    parser.add_argument("--output-name", type=str, default="")
    args = parser.parse_args()

    workflow = DeepResearchWorkflow(load_config(args.config))
    summary = workflow.run(build_task(args))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
