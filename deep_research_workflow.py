#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from deep_research import DeepResearchWorkflow, ResearchTask, WorkflowConfig


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "output"


class ChineseArgumentParser(argparse.ArgumentParser):
    def format_help(self) -> str:
        text = super().format_help()
        replacements = {
            "usage: ": "用法：",
            "options:\n": "可选参数：\n",
            "positional arguments:\n": "位置参数：\n",
            "show this help message and exit": "显示这条帮助信息并退出",
        }
        for source, target in replacements.items():
            text = text.replace(source, target)
        return text


DEFAULT_CONFIG = {
    "project_name": "行业深度研究工作流",
    "outdir": str(DEFAULT_OUTPUT_DIR / "deep_research"),
    "memory_path": str(DEFAULT_OUTPUT_DIR / "deep_research" / "workflow_memory.json"),
    "trace_path": str(DEFAULT_OUTPUT_DIR / "deep_research" / "workflow_trace.jsonl"),
    "literature_csv": str(DEFAULT_OUTPUT_DIR / "grounded_monitor" / "literature_table.csv"),
    "theme_memory_path": str(DEFAULT_OUTPUT_DIR / "grounded_monitor" / "theme_memory.json"),
    "compact_context_path": str(DEFAULT_OUTPUT_DIR / "grounded_monitor" / "compact_context.md"),
    "local_text_paths": [
        str(DEFAULT_OUTPUT_DIR / "grounded_monitor"),
    ],
    "structured_data_paths": [],
    "local_pdf_paths": [
        str(BASE_DIR / "paper_fetch_output" / "pdfs"),
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
    mode = {"报告": "report", "分析": "analysis"}.get(args.mode, args.mode)
    return ResearchTask(
        task=args.task,
        mode=mode,
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
    parser = ChineseArgumentParser(description="端到端多智能体深度研究工作流")
    parser.add_argument("--config", type=Path, default=None, help="可选的 JSON 配置文件路径")
    parser.add_argument("--task", type=str, required=True, help="研究任务或报告请求")
    parser.add_argument("--mode", type=str, default="report", choices=["report", "analysis", "报告", "分析"], help="运行模式：report/报告 或 analysis/分析")
    parser.add_argument("--symbols", type=str, default="", help="逗号分隔的标的代码，例如 600519,00700,TSLA")
    parser.add_argument("--metrics", type=str, default="", help="逗号分隔的指标列表")
    parser.add_argument("--keywords", type=str, default="", help="逗号分隔的检索关键词")
    parser.add_argument("--market-scope", type=str, default="", help="逗号分隔的市场范围")
    parser.add_argument("--report-style", type=str, default="professional", help="报告风格，例如 professional 或 专业版")
    parser.add_argument("--output-name", type=str, default="")
    args = parser.parse_args()

    workflow = DeepResearchWorkflow(load_config(args.config))
    summary = workflow.run(build_task(args))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
