from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from research_batching import (
    DEFAULT_VARIABLE_PROMPT_TEMPLATE,
    INTERVIEW_SUFFIXES,
    META_ANALYSIS_SUFFIXES,
    PAPER_CODING_SUFFIXES,
    build_stage1_dataframe,
    build_batch_symlink_folders,
    create_attachment_preview,
    build_interview_segments,
    build_meta_analysis_template,
    list_desktop_directories,
    normalize_input_paths,
    scan_source_files,
    save_stage1_outputs,
    split_into_batches,
    write_inventory,
)


APP_ROOT = Path(__file__).resolve().parent
RUNTIME_ROOT = APP_ROOT / "webui_runtime"
RUNS_ROOT = RUNTIME_ROOT / "runs"
PYTHON_BIN = sys.executable

POLICY_CORE_ITEMS = [
    {
        "年份": "2017",
        "名称": "新一代人工智能发展规划",
        "层级": "国家级核心政策",
        "发布机构": "国务院",
        "摘要": "提出我国人工智能发展的总体目标、重点任务和保障措施，是人工智能国家战略的重要纲领。",
        "链接": "https://www.gov.cn/zhengce/content/2017-07/20/content_5211996.htm",
    },
    {
        "年份": "2021",
        "名称": "“十四五”数字经济发展规划",
        "层级": "国家级核心政策",
        "发布机构": "国务院",
        "摘要": "将智能化转型、数字技术创新和产业融合纳入中长期发展体系，为人工智能应用扩展提供政策基础。",
        "链接": "https://www.gov.cn/zhengce/content/2022-01/12/content_5667817.htm",
    },
    {
        "年份": "2022",
        "名称": "关于加快场景创新以人工智能高水平应用促进经济高质量发展的指导意见",
        "层级": "国家级核心政策",
        "发布机构": "科技部等六部门",
        "摘要": "强调通过场景开放、示范应用和重点行业落地，推动人工智能由技术突破走向规模应用。",
        "链接": "https://www.gov.cn/zhengce/zhengceku/2022-08/14/content_5705342.htm",
    },
    {
        "年份": "2023",
        "名称": "生成式人工智能服务管理暂行办法",
        "层级": "国家级核心政策",
        "发布机构": "国家网信办等七部门",
        "摘要": "围绕生成式人工智能服务的提供、训练数据、内容安全和主体责任进行规范，是生成式 AI 的关键治理政策。",
        "链接": "https://www.gov.cn/zhengce/zhengceku/2023-07/13/content_6891752.htm",
    },
]

POLICY_ALL_ITEMS = [
    {
        "年份": "2015",
        "名称": "促进大数据发展行动纲要",
        "类型": "相关基础政策",
        "发布机构": "国务院",
        "摘要": "从数据资源体系建设和应用创新角度为人工智能发展奠定基础设施和数据要素条件。",
        "链接": "https://www.gov.cn/zhengce/content/2015-09/05/content_10137.htm",
    },
    {
        "年份": "2017",
        "名称": "新一代人工智能发展规划",
        "类型": "核心政策",
        "发布机构": "国务院",
        "摘要": "明确人工智能发展路线图、重点方向和战略布局。",
        "链接": "https://www.gov.cn/zhengce/content/2017-07/20/content_5211996.htm",
    },
    {
        "年份": "2018",
        "名称": "高等学校人工智能创新行动计划",
        "类型": "教育与创新政策",
        "发布机构": "教育部",
        "摘要": "推动高校人工智能学科建设、人才培养与科研创新能力提升。",
        "链接": "https://www.moe.gov.cn/srcsite/A16/s7062/201804/t20180410_332722.html",
    },
    {
        "年份": "2021",
        "名称": "“十四五”数字经济发展规划",
        "类型": "数字经济政策",
        "发布机构": "国务院",
        "摘要": "推动数字产业化与产业数字化，为人工智能落地提供制度与产业支撑。",
        "链接": "https://www.gov.cn/zhengce/content/2022-01/12/content_5667817.htm",
    },
    {
        "年份": "2022",
        "名称": "关于加快场景创新以人工智能高水平应用促进经济高质量发展的指导意见",
        "类型": "应用场景政策",
        "发布机构": "科技部等六部门",
        "摘要": "聚焦制造、能源、政务、医疗等重点场景推动人工智能示范落地。",
        "链接": "https://www.gov.cn/zhengce/zhengceku/2022-08/14/content_5705342.htm",
    },
    {
        "年份": "2023",
        "名称": "生成式人工智能服务管理暂行办法",
        "类型": "治理与监管政策",
        "发布机构": "国家网信办等七部门",
        "摘要": "系统规定生成式人工智能服务规范、内容安全与治理要求。",
        "链接": "https://www.gov.cn/zhengce/zhengceku/2023-07/13/content_6891752.htm",
    },
]

POLICY_NEWS_ITEMS = [
    {
        "日期": "2026-04-10",
        "标题": "国务院政策文件库",
        "来源": "Gov.cn",
        "摘要": "后续每日抓取会优先汇总这里发布的人工智能相关政策、意见、通知与解读。",
        "链接": "https://www.gov.cn/zhengce/index.htm",
    },
    {
        "日期": "2026-04-10",
        "标题": "中国政府网政策搜索",
        "来源": "Gov.cn Search",
        "摘要": "后续会按“人工智能、生成式人工智能、智能制造、算力、数据要素”等关键词自动检索。",
        "链接": "https://sousuo.www.gov.cn/search-gov/data",
    },
    {
        "日期": "2026-04-10",
        "标题": "国家网信办政策发布",
        "来源": "CAC",
        "摘要": "适合补充生成式 AI、算法治理、数据安全等监管类政策和新闻。",
        "链接": "https://www.cac.gov.cn/",
    },
]

POLICY_OUTPUT_ROOT = APP_ROOT / "output" / "policy_digest"
POLICY_OVERRIDES_PATH = POLICY_OUTPUT_ROOT / "policy_overrides.json"
POLICY_RELATION_RULES = [
    {
        "theme": "生成式人工智能治理",
        "keywords": ["生成式人工智能", "大模型", "服务管理", "算法治理"],
        "antecedent": ["监管规则", "治理要求", "合规压力"],
        "mechanism": ["组织信任", "合规能力", "技术采纳意愿"],
        "outcome": ["人工智能采纳", "技术应用绩效"],
        "suggestion": "把生成式人工智能监管要求作为制度环境变量，引入合规能力或组织信任，检验其对 AI 采纳与绩效的影响。",
    },
    {
        "theme": "场景创新与政策推动",
        "keywords": ["场景创新", "示范应用", "试点", "应用场景"],
        "antecedent": ["政策支持", "场景开放", "政府引导"],
        "mechanism": ["资源获取", "组织学习", "创新协同"],
        "outcome": ["AI采纳强度", "创新绩效"],
        "suggestion": "把场景开放或政策支持视为前因变量，关注其通过组织学习、资源获取影响 AI 采纳和创新绩效。",
    },
    {
        "theme": "算力与基础设施供给",
        "keywords": ["算力", "智算", "数据中心", "基础设施", "网络"],
        "antecedent": ["算力供给", "数字基础设施", "技术资源可得性"],
        "mechanism": ["技术能力", "数据处理能力", "组织数字能力"],
        "outcome": ["AI应用深度", "数字化转型绩效"],
        "suggestion": "把算力供给和基础设施条件纳入技术环境变量，检验其通过组织数字能力影响 AI 应用深度。",
    },
    {
        "theme": "数据要素与安全治理",
        "keywords": ["数据要素", "数据安全", "隐私", "语料", "信息保护"],
        "antecedent": ["数据治理要求", "数据安全压力", "数据资源质量"],
        "mechanism": ["数据能力", "风险感知", "治理成熟度"],
        "outcome": ["AI采纳意愿", "组织绩效"],
        "suggestion": "把数据安全与数据治理要求纳入前因或边界条件，关注其对 AI 采纳意愿和组织绩效的影响。",
    },
    {
        "theme": "智能制造与产业升级",
        "keywords": ["智能制造", "工业", "机器人", "产业升级"],
        "antecedent": ["产业政策支持", "制造场景需求", "技术升级压力"],
        "mechanism": ["流程重构", "组织能力升级", "技术整合"],
        "outcome": ["生产效率", "企业绩效", "竞争优势"],
        "suggestion": "将智能制造政策纳入行业情境变量，检验其通过流程重构和能力升级作用于绩效提升。",
    },
]


def ensure_runtime_dirs() -> None:
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)


def timestamp_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def safe_name(text: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text.strip())
    return cleaned.strip("_") or "run"


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def extract_last_json_block(text: str) -> dict[str, Any]:
    lines = [line for line in text.splitlines() if line.strip()]
    for start in range(len(lines)):
        candidate = "\n".join(lines[start:])
        try:
            return json.loads(candidate)
        except Exception:
            continue
    return {}


def run_command(args: list[str], env_overrides: dict[str, str] | None = None) -> tuple[int, str, str]:
    env = os.environ.copy()
    if env_overrides:
        env.update({key: value for key, value in env_overrides.items() if value})
    process = subprocess.run(
        args,
        cwd=APP_ROOT,
        text=True,
        capture_output=True,
        env=env,
    )
    return process.returncode, process.stdout, process.stderr


def save_uploaded_files(files: list[Any], target_dir: Path) -> list[str]:
    saved: list[str] = []
    target_dir.mkdir(parents=True, exist_ok=True)
    for uploaded in files:
        destination = target_dir / uploaded.name
        destination.write_bytes(uploaded.getbuffer())
        saved.append(str(destination))
    return saved


def as_file_uri(path_text: str) -> str:
    if not path_text:
        return ""
    return Path(path_text).resolve().as_uri()


def renumber_rows(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty or "序号" not in dataframe.columns:
        return dataframe
    copy = dataframe.copy()
    copy["序号"] = list(range(1, len(copy) + 1))
    return copy


def enrich_stage1_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    copy = dataframe.copy()
    if "文件路径" not in copy.columns:
        copy["文件路径"] = ""
    if "附件预览" not in copy.columns:
        copy["附件预览"] = ""
    if "查看原文" not in copy.columns:
        copy["查看原文"] = copy["文件路径"].fillna("").map(as_file_uri)
    else:
        missing_mask = copy["查看原文"].fillna("").astype(str).str.strip() == ""
        copy.loc[missing_mask, "查看原文"] = copy.loc[missing_mask, "文件路径"].fillna("").map(as_file_uri)
    return copy


def reorder_files_by_upload_order(
    files: list[Any],
    uploaded_files: list[Any],
    preferred_names: list[str] | None = None,
) -> list[Any]:
    if preferred_names:
        upload_order = {name: index for index, name in enumerate(preferred_names)}
    elif uploaded_files:
        upload_order = {uploaded.name: index for index, uploaded in enumerate(uploaded_files)}
    else:
        return files
    ordered: list[tuple[int, int, Any]] = []
    for index, item in enumerate(files):
        priority = upload_order.get(Path(item.path).name, len(upload_order) + index)
        ordered.append((priority, index, item))
    ordered.sort(key=lambda row: (row[0], row[1]))
    return [item for _, _, item in ordered]


def build_attachment_seed_table(uploaded_files: list[Any], preview_root: Path, rows: int = 18) -> pd.DataFrame:
    prompt = "我将按照学术文献解构框架，系统分析这篇关于企业人工智能采纳的实证研究论文。"
    upload_dir = preview_root / "seed_uploads"
    saved_paths = save_uploaded_files(uploaded_files, upload_dir)
    preview_dir = preview_root / "seed_previews"
    attachment_rows: list[dict[str, Any]] = []
    for saved_path in saved_paths:
        path = Path(saved_path)
        attachment_rows.append(
            {
                "附件预览": create_attachment_preview(path, preview_dir),
                "附件": path.name,
                "文件路径": str(path),
                "查看原文": as_file_uri(str(path)),
            }
        )
    total_rows = max(rows, len(attachment_rows) + 8)
    data = []
    for index in range(1, total_rows + 1):
        attachment = attachment_rows[index - 1] if index - 1 < len(attachment_rows) else {}
        data.append(
            {
                "序号": index,
                "附件预览": attachment.get("附件预览", ""),
                "附件": attachment.get("附件", ""),
                "文件路径": attachment.get("文件路径", ""),
                "查看原文": attachment.get("查看原文", ""),
                "主要概念": "",
                "主要观点": "",
                "变量筛选prompt": prompt,
            }
        )
    return pd.DataFrame(data)


def base_grounded_config(run_dir: Path) -> dict[str, Any]:
    return {
        "project_name": "网页端论文编码",
        "queries": [],
        "sources": ["local"],
        "days_back": 30,
        "max_results_per_query": 15,
        "download_pdfs": False,
        "max_pdf_pages_for_coding": 30,
        "sleep_seconds": 0.2,
        "outdir": str(run_dir / "grounded_output"),
        "baseline_paths": [],
        "covered_topics": [],
        "local_library_paths": [],
        "max_local_papers": 50,
        "translation": {
            "enabled": False,
            "provider": "openai_compatible",
            "model": "",
            "api_url": "",
            "api_key_env": "GROUNDED_TRANSLATE_API_KEY",
        },
        "skill_dirs": [],
        "agent": {
            "enabled": True,
            "max_turns": 12,
            "trace_candidates": False,
        },
        "assistant": {
            "enabled": False,
            "provider": "openai_compatible",
            "model": "",
            "api_url": "",
            "api_key_env": "GROUNDED_AGENT_API_KEY",
            "temperature": 0.2,
            "max_context_rows": 10,
            "max_context_chars": 24000,
            "answer_dirname": "qa_answers",
            "report_dirname": "industry_reports",
        },
        "context_compression": {
            "enabled": True,
            "char_threshold": 30000,
            "max_recent_rows": 12,
        },
    }


def base_deep_research_config(run_dir: Path, literature_csv: str = "") -> dict[str, Any]:
    output_dir = run_dir / "deep_research_output"
    grounded_dir = run_dir / "paper_coding_merged"
    return {
        "project_name": "网页端行业深度研究工作流",
        "outdir": str(output_dir),
        "memory_path": str(output_dir / "workflow_memory.json"),
        "trace_path": str(output_dir / "workflow_trace.jsonl"),
        "literature_csv": literature_csv,
        "theme_memory_path": str(grounded_dir / "theme_memory.json"),
        "compact_context_path": str(grounded_dir / "compact_context.md"),
        "local_text_paths": [str(grounded_dir)],
        "structured_data_paths": [],
        "local_pdf_paths": [str(run_dir / "uploaded_pdfs")],
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
                "enable_symbol_financials": False,
            },
            "news": {"provider": "google_news_rss", "max_items": 8},
            "policy": {"provider": "gov_cn_policy_library", "max_items": 8},
            "community": {
                "providers": ["eastmoney_guba", "stocktwits"],
                "max_items": 8,
                "per_symbol_limit": 3,
            },
        },
    }


def render_download(path: Path, label: str, mime: str) -> None:
    if not path.exists():
        return
    st.download_button(label, data=path.read_bytes(), file_name=path.name, mime=mime)


def render_markdown_file(path: Path, title: str) -> None:
    if not path.exists():
        return
    st.markdown(f"### {title}")
    st.markdown(path.read_text(encoding="utf-8", errors="ignore"))


def inject_policy_digest_styles() -> None:
    st.markdown(
        """
<style>
.policy-shell {
  border: 1px solid rgba(30, 64, 175, 0.10);
  border-radius: 24px;
  background: linear-gradient(135deg, #f8fbff 0%, #eef5ff 52%, #f8fffc 100%);
  padding: 24px;
  margin-bottom: 18px;
}
.policy-kicker {
  color: #1d4ed8;
  font-size: 12px;
  font-weight: 800;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  margin-bottom: 10px;
}
.policy-title {
  font-size: 31px;
  line-height: 1.15;
  font-weight: 800;
  color: #0f172a;
  margin: 0 0 10px 0;
}
.policy-subtitle {
  color: #334155;
  font-size: 15px;
  line-height: 1.75;
  margin: 0;
}
.policy-card {
  border: 1px solid rgba(30, 64, 175, 0.10);
  border-radius: 18px;
  background: white;
  padding: 18px;
  min-height: 150px;
}
.policy-chip {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  background: #dbeafe;
  color: #1d4ed8;
  padding: 5px 10px;
  font-size: 12px;
  font-weight: 700;
  margin-bottom: 10px;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def render_policy_cards(items: list[dict[str, str]], *, mode: str) -> None:
    for item in items:
        with st.container():
            col1, col2 = st.columns([4.2, 1.1])
            title = item.get("名称") or item.get("标题") or item.get("title") or "未命名条目"
            summary = item.get("摘要") or item.get("summary") or ""
            publish_label = item.get("年份") or item.get("日期") or item.get("published_at") or ""
            issuer = item.get("发布机构") or item.get("来源") or item.get("issuing_body") or item.get("source_name") or "待补充"
            meta_label = item.get("层级") or item.get("类型") or item.get("category") or item.get("来源") or "政策条目"
            url = item.get("链接") or item.get("url") or ""
            col1.markdown(
                f"""
<div class="policy-card">
  <div class="policy-chip">{publish_label} · {meta_label}</div>
  <h4 style="margin:0 0 8px 0;color:#0f172a;">{title}</h4>
  <p style="margin:0 0 8px 0;color:#475569;font-size:13px;">发布机构 / 来源：{issuer}</p>
  <p style="margin:0;color:#334155;line-height:1.75;">{summary}</p>
</div>
                """,
                unsafe_allow_html=True,
            )
            button_label = "查看政策原文" if mode != "news" else "查看新闻 / 页面"
            if url:
                col2.link_button(button_label, url, use_container_width=True)


def policy_item_key(item: dict[str, Any]) -> str:
    return str(item.get("item_id") or item.get("url") or item.get("链接") or item.get("title") or item.get("标题") or item.get("名称") or "").strip()


def apply_policy_overrides(items: list[dict[str, Any]], overrides: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    applied: list[dict[str, Any]] = []
    for item in items:
        copy = dict(item)
        key = policy_item_key(copy)
        if key and key in overrides:
            copy.update({k: v for k, v in overrides[key].items() if v not in (None, "")})
        applied.append(copy)
    return applied


def save_policy_overrides_from_rows(rows: list[dict[str, Any]]) -> None:
    overrides = read_json(POLICY_OVERRIDES_PATH, {})
    for row in rows:
        item_key = str(row.get("item_key", "")).strip()
        if not item_key:
            continue
        source_type = str(row.get("条目类型", "")).strip() or "policy"
        category = str(row.get("人工分类", "")).strip() or "相关政策"
        core_flag = str(row.get("核心政策", "")).strip() in {"是", "True", "true", "1"}
        issuer = str(row.get("人工发布机构", "")).strip()
        overrides[item_key] = {
            "source_type": source_type,
            "category": category,
            "is_core": core_flag,
            "issuing_body": issuer,
            "source_name": issuer,
        }
    write_json(POLICY_OVERRIDES_PATH, overrides)


def merge_policy_snapshot_items(policy_items: list[dict[str, Any]], news_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in policy_items + news_items:
        key = policy_item_key(item) or json.dumps(item, ensure_ascii=False, sort_keys=True)
        existing = merged.get(key)
        if existing is None or len(str(item.get("summary") or item.get("摘要") or "")) > len(str(existing.get("summary") or existing.get("摘要") or "")):
            merged[key] = dict(item)
    return list(merged.values())


def build_policy_gap_insights(stage1_rows: list[dict[str, Any]], policy_items: list[dict[str, Any]]) -> dict[str, Any]:
    if not stage1_rows or not policy_items:
        return {"gap_rows": [], "suggestions": []}
    concept_pool: set[str] = set()
    variable_pool: set[str] = set()
    indep_pool: set[str] = set()
    med_pool: set[str] = set()
    dep_pool: set[str] = set()
    relation_signatures: set[str] = set()
    for row in stage1_rows:
        for field in ["主要概念", "自变量", "中介/调节变量", "因变量/结果变量", "控制变量", "未来研究编码"]:
            raw = str(row.get(field, "")).replace("；", "、")
            for part in raw.split("、"):
                part = part.strip()
                if part and part != "待补充":
                    concept_pool.add(part)
                    variable_pool.add(part)
        for field, pool in [
            ("自变量", indep_pool),
            ("中介/调节变量", med_pool),
            ("因变量/结果变量", dep_pool),
        ]:
            raw = str(row.get(field, "")).replace("；", "、")
            values = [part.strip() for part in raw.split("、") if part.strip() and part.strip() != "待补充"]
            pool.update(values)
        indeps = [part.strip() for part in str(row.get("自变量", "")).replace("；", "、").split("、") if part.strip() and part.strip() != "待补充"]
        meds = [part.strip() for part in str(row.get("中介/调节变量", "")).replace("；", "、").split("、") if part.strip() and part.strip() != "待补充"]
        deps = [part.strip() for part in str(row.get("因变量/结果变量", "")).replace("；", "、").split("、") if part.strip() and part.strip() != "待补充"]
        for indep in indeps[:3]:
            for dep in deps[:3]:
                relation_signatures.add(f"{indep}->{dep}")
                for med in meds[:2]:
                    relation_signatures.add(f"{indep}->{med}->{dep}")

    gap_rows: list[dict[str, str]] = []
    seen_signatures: set[str] = set()
    for item in policy_items:
        text = f"{item.get('title', item.get('名称', item.get('标题', '')))} {item.get('summary', item.get('摘要', ''))}"
        for rule in POLICY_RELATION_RULES:
            matched_terms = [term for term in rule["keywords"] if term in text]
            if not matched_terms:
                continue
            antecedent_terms = rule["antecedent"]
            mechanism_terms = rule["mechanism"]
            outcome_terms = rule["outcome"]
            antecedent_covered = any(term in indep_pool or term in concept_pool for term in antecedent_terms)
            mechanism_covered = any(term in med_pool or term in concept_pool for term in mechanism_terms)
            outcome_covered = any(term in dep_pool or term in concept_pool for term in outcome_terms)
            signature = f"{rule['theme']}|{'/'.join(antecedent_terms[:1])}|{'/'.join(mechanism_terms[:1])}|{'/'.join(outcome_terms[:1])}"
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            if antecedent_covered and outcome_covered and mechanism_covered:
                continue
            coverage_parts = []
            coverage_parts.append("前因已覆盖" if antecedent_covered else "前因缺口")
            coverage_parts.append("机制已覆盖" if mechanism_covered else "机制缺口")
            coverage_parts.append("结果已覆盖" if outcome_covered else "结果缺口")
            missing_count = sum([not antecedent_covered, not mechanism_covered, not outcome_covered])
            level = "高" if missing_count >= 2 else "中"
            gap_rows.append(
                {
                    "政策主题": rule["theme"],
                    "政策命中词": "、".join(matched_terms),
                    "建议关系链": f"{antecedent_terms[0]} -> {mechanism_terms[0]} -> {outcome_terms[0]}",
                    "前因变量": "、".join(antecedent_terms),
                    "机制/调节变量": "、".join(mechanism_terms),
                    "结果变量": "、".join(outcome_terms),
                    "覆盖情况": "｜".join(coverage_parts),
                    "缺口级别": level,
                    "对应政策": item.get("title") or item.get("名称") or item.get("标题") or "",
                    "补充方向": rule["suggestion"],
                }
            )

    suggestions = [row["补充方向"] for row in gap_rows[:5]]
    return {"gap_rows": gap_rows, "suggestions": suggestions}


def build_policy_proposition_drafts(gap_insights: dict[str, Any]) -> dict[str, Any]:
    gap_rows = gap_insights.get("gap_rows", [])
    proposition_rows: list[dict[str, str]] = []
    for index, row in enumerate(gap_rows, start=1):
        antecedent = str(row.get("前因变量", "")).split("、")[0].strip() or "前因变量"
        mechanism = str(row.get("机制/调节变量", "")).split("、")[0].strip() or "机制变量"
        outcome = str(row.get("结果变量", "")).split("、")[0].strip() or "结果变量"
        theme = str(row.get("政策主题", "")).strip() or "政策主题"
        policy_name = str(row.get("对应政策", "")).strip() or "相关政策"
        coverage = str(row.get("覆盖情况", "")).strip()
        level = str(row.get("缺口级别", "")).strip() or "中"
        proposition_text = (
            f"命题 P{index}：在“{theme}”政策情境下，{antecedent}会通过{mechanism}进一步影响{outcome}。"
            f"该命题可结合“{policy_name}”所体现的制度环境变化进行检验。"
        )
        if "机制缺口" in coverage and "前因缺口" not in coverage:
            position = "建议放入理论机制或研究假设部分，补强中介 / 调节路径。"
        elif "前因缺口" in coverage:
            position = "建议放入研究框架前因部分，把政策变量正式引入模型。"
        elif "结果缺口" in coverage:
            position = "建议放入结果讨论或扩展模型部分，补充政策作用结果变量。"
        else:
            position = "建议放入研究假设与讨论衔接处，作为扩展命题。"
        proposition_rows.append(
            {
                "命题编号": f"P{index}",
                "政策主题": theme,
                "建议关系链": str(row.get("建议关系链", "")),
                "研究命题草案": proposition_text,
                "适合放入论文位置": position,
                "缺口级别": level,
                "对应政策": policy_name,
                "补充方向": str(row.get("补充方向", "")),
            }
        )
    return {"rows": proposition_rows}


def build_policy_hypothesis_drafts(gap_insights: dict[str, Any]) -> dict[str, Any]:
    gap_rows = gap_insights.get("gap_rows", [])
    hypothesis_rows: list[dict[str, str]] = []
    for index, row in enumerate(gap_rows, start=1):
        antecedent = str(row.get("前因变量", "")).split("、")[0].strip() or "前因变量"
        mechanism = str(row.get("机制/调节变量", "")).split("、")[0].strip() or "机制变量"
        outcome = str(row.get("结果变量", "")).split("、")[0].strip() or "结果变量"
        theme = str(row.get("政策主题", "")).strip() or "政策主题"
        policy_name = str(row.get("对应政策", "")).strip() or "相关政策"
        coverage = str(row.get("覆盖情况", "")).strip()
        if "机制缺口" in coverage and "前因缺口" not in coverage:
            hypothesis_type = "中介 / 调节假设"
            statement = f"H{index}：在“{theme}”政策情境下，{antecedent}通过{mechanism}正向影响{outcome}。"
        elif "前因缺口" in coverage:
            hypothesis_type = "主效应假设"
            statement = f"H{index}：在“{theme}”政策情境下，{antecedent}正向影响{outcome}。"
        elif "结果缺口" in coverage:
            hypothesis_type = "结果效应假设"
            statement = f"H{index}：在“{theme}”政策情境下，{mechanism}正向影响{outcome}。"
        else:
            hypothesis_type = "扩展假设"
            statement = f"H{index}：在“{theme}”政策情境下，{antecedent}通过{mechanism}进一步影响{outcome}。"
        hypothesis_rows.append(
            {
                "假设编号": f"H{index}",
                "政策主题": theme,
                "假设类型": hypothesis_type,
                "建议关系链": str(row.get("建议关系链", "")),
                "研究假设草案": statement,
                "理论依据": f"可结合“{policy_name}”所体现的制度环境变化与政策压力展开论证。",
                "对应政策": policy_name,
                "补充方向": str(row.get("补充方向", "")),
            }
        )
    return {"rows": hypothesis_rows}


def build_policy_context_bundle(target_dir: Path) -> dict[str, str]:
    snapshot = load_policy_digest_snapshot()
    core_policies = snapshot["core_policies"]
    all_policies = snapshot["all_policies"]
    news_updates = snapshot["news_updates"]
    context_dir = target_dir / "policy_context"
    context_dir.mkdir(parents=True, exist_ok=True)
    policy_md = context_dir / "policy_context.md"
    news_md = context_dir / "news_context.md"
    combined_md = context_dir / "policy_news_digest.md"

    policy_lines = ["# 人工智能政策补充材料", ""]
    for item in core_policies[:20]:
        title = item.get("title") or item.get("名称") or item.get("标题") or "未命名条目"
        url = item.get("url") or item.get("链接") or ""
        issuer = item.get("issuing_body") or item.get("发布机构") or item.get("来源") or ""
        summary = item.get("summary") or item.get("摘要") or ""
        policy_lines.extend([f"## {title}", "", f"- 发布机构：{issuer}", f"- 链接：{url}", f"- 摘要：{summary}", ""])
    news_lines = ["# 人工智能政策新闻补充材料", ""]
    for item in news_updates[:20]:
        title = item.get("title") or item.get("名称") or item.get("标题") or "未命名条目"
        url = item.get("url") or item.get("链接") or ""
        source = item.get("source_name") or item.get("来源") or ""
        summary = item.get("summary") or item.get("摘要") or ""
        news_lines.extend([f"## {title}", "", f"- 来源：{source}", f"- 链接：{url}", f"- 摘要：{summary}", ""])
    combined_lines = [
        "# 人工智能政策与新闻研究补充包",
        "",
        f"- 核心政策数：{len(core_policies)}",
        f"- 全部政策数：{len(all_policies)}",
        f"- 新闻 / 解读数：{len(news_updates)}",
        "",
    ]
    combined_lines.extend(policy_lines[2:])
    combined_lines.extend(news_lines[2:])
    policy_md.write_text("\n".join(policy_lines).strip() + "\n", encoding="utf-8")
    news_md.write_text("\n".join(news_lines).strip() + "\n", encoding="utf-8")
    combined_md.write_text("\n".join(combined_lines).strip() + "\n", encoding="utf-8")
    return {
        "policy_md": str(policy_md),
        "news_md": str(news_md),
        "combined_md": str(combined_md),
        "context_dir": str(context_dir),
    }


def save_policy_gap_analysis_bundle(snapshot: dict[str, Any], gap_insights: dict[str, Any]) -> dict[str, str]:
    latest_dir = snapshot["latest_dir"]
    latest_dir.mkdir(parents=True, exist_ok=True)
    csv_path = latest_dir / "policy_paper_gap_analysis.csv"
    xlsx_path = latest_dir / "policy_paper_gap_analysis.xlsx"
    md_path = latest_dir / "policy_paper_gap_analysis.md"
    rows = gap_insights.get("gap_rows", [])
    dataframe = pd.DataFrame(rows)
    if not dataframe.empty:
        dataframe.to_csv(csv_path, index=False)
        dataframe.to_excel(xlsx_path, index=False)
    else:
        pd.DataFrame(
            columns=[
                "政策主题",
                "政策命中词",
                "建议关系链",
                "前因变量",
                "机制/调节变量",
                "结果变量",
                "覆盖情况",
                "缺口级别",
                "对应政策",
                "补充方向",
            ]
        ).to_csv(csv_path, index=False)
        pd.DataFrame().to_excel(xlsx_path, index=False)

    lines = [
        "# 政策-论文缺口分析表",
        "",
        f"- 生成时间：{datetime.now().isoformat(timespec='seconds')}",
        f"- 关系链缺口数：{len(rows)}",
        "",
    ]
    if rows:
        for row in rows:
            lines.extend(
                [
                    f"## {row['政策主题']}",
                    "",
                    f"- 建议关系链：{row['建议关系链']}",
                    f"- 覆盖情况：{row['覆盖情况']}",
                    f"- 缺口级别：{row['缺口级别']}",
                    f"- 对应政策：{row['对应政策']}",
                    f"- 补充方向：{row['补充方向']}",
                    "",
                ]
            )
    else:
        lines.append("当前没有识别到明显的政策-论文关系链缺口。")
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"csv": str(csv_path), "xlsx": str(xlsx_path), "md": str(md_path)}


def save_policy_proposition_bundle(snapshot: dict[str, Any], proposition_drafts: dict[str, Any]) -> dict[str, str]:
    latest_dir = snapshot["latest_dir"]
    latest_dir.mkdir(parents=True, exist_ok=True)
    csv_path = latest_dir / "policy_proposition_drafts.csv"
    xlsx_path = latest_dir / "policy_proposition_drafts.xlsx"
    md_path = latest_dir / "policy_proposition_drafts.md"
    rows = proposition_drafts.get("rows", [])
    dataframe = pd.DataFrame(rows)
    if not dataframe.empty:
        dataframe.to_csv(csv_path, index=False)
        dataframe.to_excel(xlsx_path, index=False)
    else:
        empty = pd.DataFrame(
            columns=[
                "命题编号",
                "政策主题",
                "建议关系链",
                "研究命题草案",
                "适合放入论文位置",
                "缺口级别",
                "对应政策",
                "补充方向",
            ]
        )
        empty.to_csv(csv_path, index=False)
        empty.to_excel(xlsx_path, index=False)

    lines = [
        "# 政策驱动研究命题草案",
        "",
        f"- 生成时间：{datetime.now().isoformat(timespec='seconds')}",
        f"- 命题草案数：{len(rows)}",
        "",
    ]
    if rows:
        for row in rows:
            lines.extend(
                [
                    f"## {row['命题编号']}｜{row['政策主题']}",
                    "",
                    f"- 建议关系链：{row['建议关系链']}",
                    f"- 研究命题草案：{row['研究命题草案']}",
                    f"- 适合放入论文位置：{row['适合放入论文位置']}",
                    f"- 缺口级别：{row['缺口级别']}",
                    f"- 对应政策：{row['对应政策']}",
                    f"- 补充方向：{row['补充方向']}",
                    "",
                ]
            )
    else:
        lines.extend(["当前没有可转写的政策驱动研究命题草案。", ""])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"csv": str(csv_path), "xlsx": str(xlsx_path), "md": str(md_path)}


def save_policy_hypothesis_bundle(snapshot: dict[str, Any], hypothesis_drafts: dict[str, Any]) -> dict[str, str]:
    latest_dir = snapshot["latest_dir"]
    latest_dir.mkdir(parents=True, exist_ok=True)
    csv_path = latest_dir / "policy_hypothesis_drafts.csv"
    xlsx_path = latest_dir / "policy_hypothesis_drafts.xlsx"
    md_path = latest_dir / "policy_hypothesis_drafts.md"
    rows = hypothesis_drafts.get("rows", [])
    dataframe = pd.DataFrame(rows)
    if not dataframe.empty:
        dataframe.to_csv(csv_path, index=False)
        dataframe.to_excel(xlsx_path, index=False)
    else:
        empty = pd.DataFrame(
            columns=[
                "假设编号",
                "政策主题",
                "假设类型",
                "建议关系链",
                "研究假设草案",
                "理论依据",
                "对应政策",
                "补充方向",
            ]
        )
        empty.to_csv(csv_path, index=False)
        empty.to_excel(xlsx_path, index=False)

    lines = [
        "# 政策驱动研究假设草案",
        "",
        f"- 生成时间：{datetime.now().isoformat(timespec='seconds')}",
        f"- 假设草案数：{len(rows)}",
        "",
    ]
    if rows:
        for row in rows:
            lines.extend(
                [
                    f"## {row['假设编号']}｜{row['政策主题']}",
                    "",
                    f"- 假设类型：{row['假设类型']}",
                    f"- 建议关系链：{row['建议关系链']}",
                    f"- 研究假设草案：{row['研究假设草案']}",
                    f"- 理论依据：{row['理论依据']}",
                    f"- 对应政策：{row['对应政策']}",
                    f"- 补充方向：{row['补充方向']}",
                    "",
                ]
            )
    else:
        lines.extend(["当前没有可转写的政策驱动研究假设草案。", ""])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return {"csv": str(csv_path), "xlsx": str(xlsx_path), "md": str(md_path)}


def load_latest_stage1_rows() -> list[dict[str, Any]]:
    stage1_csv = st.session_state.get("latest_paper_stage1_csv", "")
    if stage1_csv and Path(stage1_csv).exists():
        try:
            return pd.read_csv(stage1_csv).fillna("").to_dict(orient="records")
        except Exception:
            return []
    latest_run = st.session_state.get("latest_paper_coding_run", "")
    if latest_run:
        candidate = Path(latest_run) / "stage1_outputs" / "paper_stage1_table.csv"
        if candidate.exists():
            try:
                return pd.read_csv(candidate).fillna("").to_dict(orient="records")
            except Exception:
                return []
    return []


def policy_snapshot_available() -> bool:
    latest_dir = POLICY_OUTPUT_ROOT / "latest"
    return (latest_dir / "all_policies.json").exists() or (latest_dir / "news_updates.json").exists()


def load_policy_digest_snapshot() -> dict[str, Any]:
    latest_dir = POLICY_OUTPUT_ROOT / "latest"
    summary = read_json(latest_dir / "summary.json", {})
    overrides = read_json(POLICY_OVERRIDES_PATH, {})
    run_date = str(summary.get("run_at", datetime.now().isoformat()))[:10]
    merged_items = merge_policy_snapshot_items(
        read_json(latest_dir / "all_policies.json", POLICY_ALL_ITEMS),
        read_json(latest_dir / "news_updates.json", POLICY_NEWS_ITEMS),
    )
    merged_items = apply_policy_overrides(merged_items, overrides)
    all_policies = [item for item in merged_items if str(item.get("source_type", "policy")) == "policy"]
    core_policies = [item for item in all_policies if bool(item.get("is_core", item.get("层级") == "国家级核心政策"))]
    news_updates = [item for item in merged_items if str(item.get("source_type", "policy")) != "policy"]
    daily_updates = apply_policy_overrides(read_json(latest_dir / "daily_updates.json", []), overrides)
    return {
        "summary": summary,
        "core_policies": core_policies or POLICY_CORE_ITEMS,
        "all_policies": all_policies or POLICY_ALL_ITEMS,
        "news_updates": news_updates or POLICY_NEWS_ITEMS,
        "daily_updates": daily_updates,
        "latest_dir": latest_dir,
        "daily_digest_path": latest_dir / f"daily_digest_{run_date}.md",
        "overrides": overrides,
    }


def run_policy_digest_fetch() -> dict[str, Any]:
    POLICY_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    code, stdout, stderr = run_command(
        [
            PYTHON_BIN,
            "policy_digest_fetcher.py",
            "--outdir",
            str(POLICY_OUTPUT_ROOT),
        ]
    )
    return {
        "code": code,
        "stdout": stdout,
        "stderr": stderr,
        "summary": extract_last_json_block(stdout),
    }


def render_policy_correction_panel(snapshot: dict[str, Any]) -> None:
    merged_items = merge_policy_snapshot_items(snapshot["all_policies"], snapshot["news_updates"])
    merged_items = sorted(
        merged_items,
        key=lambda item: (
            0 if bool(item.get("is_core", False)) else 1,
            str(item.get("published_at", item.get("年份", ""))),
            str(item.get("title") or item.get("名称") or item.get("标题") or ""),
        ),
        reverse=True,
    )
    st.markdown("### 人工校正面板")
    st.caption("这里可以对自动分类结果做人工修正。保存后，核心政策、全部政策和每日抓取页会立即按你的结果刷新。")
    if not merged_items:
        st.info("当前还没有可校正的政策或新闻条目。先点击上方“立即抓取政策与新闻”。")
        return

    preview_rows: list[dict[str, Any]] = []
    for item in merged_items[:80]:
        preview_rows.append(
            {
                "item_key": policy_item_key(item),
                "标题": item.get("title") or item.get("名称") or item.get("标题") or "",
                "来源": item.get("source_name") or item.get("来源") or item.get("发布机构") or item.get("issuing_body") or "",
                "条目类型": item.get("source_type", ""),
                "人工分类": item.get("category", item.get("类型", "")),
                "核心政策": "是" if bool(item.get("is_core", False)) else "否",
                "人工发布机构": item.get("issuing_body") or item.get("发布机构") or item.get("source_name") or item.get("来源") or "",
                "规则命中": "；".join(item.get("rule_hits") or []),
            }
        )
    st.markdown("#### 批量校正表格")
    edited_rows = st.data_editor(
        pd.DataFrame(preview_rows),
        use_container_width=True,
        hide_index=True,
        height=520,
        num_rows="dynamic",
        column_config={
            "item_key": st.column_config.TextColumn("item_key", disabled=True, width="small"),
            "标题": st.column_config.TextColumn("标题", disabled=True, width="large"),
            "来源": st.column_config.TextColumn("来源", disabled=True, width="medium"),
            "条目类型": st.column_config.SelectboxColumn("条目类型", options=["policy", "news"], width="small"),
            "人工分类": st.column_config.SelectboxColumn("人工分类", options=["核心政策", "相关政策", "政策解读 / 新闻", "相关新闻"], width="medium"),
            "核心政策": st.column_config.SelectboxColumn("核心政策", options=["是", "否"], width="small"),
            "人工发布机构": st.column_config.TextColumn("人工发布机构", width="medium"),
            "规则命中": st.column_config.TextColumn("规则命中", disabled=True, width="large"),
        },
        key="policy-batch-editor",
    )
    batch_col1, batch_col2 = st.columns(2)
    if batch_col1.button("保存批量校正结果", type="primary", use_container_width=True):
        save_policy_overrides_from_rows(pd.DataFrame(edited_rows).to_dict(orient="records"))
        st.success("批量校正结果已保存。")
        st.rerun()
    if batch_col2.button("清空全部人工校正", use_container_width=True):
        write_json(POLICY_OVERRIDES_PATH, {})
        st.success("人工校正记录已清空。")
        st.rerun()

    st.markdown("---")
    st.markdown("#### 单条精细校正")
    option_map = {}
    option_labels = []
    for item in merged_items:
        title = item.get("title") or item.get("名称") or item.get("标题") or "未命名条目"
        source_name = item.get("source_name") or item.get("来源") or item.get("发布机构") or item.get("issuing_body") or "未知来源"
        label = f"{title}｜{source_name}"
        option_labels.append(label)
        option_map[label] = item

    selected_label = st.selectbox("选择要校正的条目", options=option_labels, key="policy-correction-item")
    selected_item = option_map[selected_label]
    item_key = policy_item_key(selected_item)
    existing_override = snapshot.get("overrides", {}).get(item_key, {})

    form_col1, form_col2 = st.columns(2)
    corrected_source_type = form_col1.selectbox(
        "条目类型",
        options=["policy", "news"],
        index=0 if str(existing_override.get("source_type", selected_item.get("source_type", "policy"))) == "policy" else 1,
        key=f"policy-source-type-{item_key}",
    )
    corrected_category = form_col2.selectbox(
        "分类",
        options=["核心政策", "相关政策", "政策解读 / 新闻", "相关新闻"],
        index=["核心政策", "相关政策", "政策解读 / 新闻", "相关新闻"].index(
            str(existing_override.get("category", selected_item.get("category", "相关政策")))
            if str(existing_override.get("category", selected_item.get("category", "相关政策"))) in ["核心政策", "相关政策", "政策解读 / 新闻", "相关新闻"]
            else "相关政策"
        ),
        key=f"policy-category-{item_key}",
    )
    corrected_core = st.checkbox(
        "标记为核心政策",
        value=bool(existing_override.get("is_core", selected_item.get("is_core", False))),
        key=f"policy-core-{item_key}",
    )
    corrected_issuer = st.text_input(
        "发布机构 / 来源",
        value=str(existing_override.get("issuing_body", selected_item.get("issuing_body") or selected_item.get("发布机构") or selected_item.get("source_name") or selected_item.get("来源") or "")),
        key=f"policy-issuer-{item_key}",
    )
    st.markdown("#### 自动规则说明")
    st.code("\n".join(selected_item.get("rule_hits") or ["当前没有记录到明确规则命中。"]))

    action_col1, action_col2 = st.columns(2)
    if action_col1.button("保存人工校正", key=f"policy-save-{item_key}", type="primary", use_container_width=True):
        overrides = read_json(POLICY_OVERRIDES_PATH, {})
        overrides[item_key] = {
            "source_type": corrected_source_type,
            "category": corrected_category,
            "is_core": corrected_core,
            "issuing_body": corrected_issuer,
            "source_name": corrected_issuer or selected_item.get("source_name", ""),
        }
        write_json(POLICY_OVERRIDES_PATH, overrides)
        st.success("人工校正已保存。")
        st.rerun()
    if action_col2.button("清除人工校正", key=f"policy-clear-{item_key}", use_container_width=True):
        overrides = read_json(POLICY_OVERRIDES_PATH, {})
        if item_key in overrides:
            overrides.pop(item_key, None)
            write_json(POLICY_OVERRIDES_PATH, overrides)
            st.success("该条目的人工校正已清除。")
            st.rerun()


def policy_digest_panel() -> None:
    inject_policy_digest_styles()
    snapshot = load_policy_digest_snapshot()
    summary = snapshot["summary"]
    core_policies = snapshot["core_policies"]
    all_policies = snapshot["all_policies"]
    news_updates = snapshot["news_updates"]
    daily_updates = snapshot["daily_updates"]
    daily_digest_path = snapshot["daily_digest_path"]
    stage1_rows = load_latest_stage1_rows()
    gap_insights = build_policy_gap_insights(stage1_rows, all_policies + news_updates)
    proposition_drafts = build_policy_proposition_drafts(gap_insights) if stage1_rows else {"rows": []}
    hypothesis_drafts = build_policy_hypothesis_drafts(gap_insights) if stage1_rows else {"rows": []}
    gap_bundle = save_policy_gap_analysis_bundle(snapshot, gap_insights) if stage1_rows else {}
    proposition_bundle = save_policy_proposition_bundle(snapshot, proposition_drafts) if stage1_rows else {}
    hypothesis_bundle = save_policy_hypothesis_bundle(snapshot, hypothesis_drafts) if stage1_rows else {}
    st.markdown(
        """
<div class="policy-shell">
  <div class="policy-kicker">Policy Intelligence</div>
  <h1 class="policy-title">人工智能政策汇总</h1>
  <p class="policy-subtitle">
    这个页面先把政策汇总的交互结构搭出来：左侧新增独立入口，页面内先分为“核心政策”和“全部政策”两层，
    同时预留每日自动抓取政策与相关新闻的状态区。后续接入真实抓取后，这里会每天自动更新并保留可点击跳转的原文入口。
  </p>
</div>
        """,
        unsafe_allow_html=True,
    )

    action_col1, action_col2, action_col3 = st.columns([1.2, 1.1, 2.2])
    if action_col1.button("立即抓取政策与新闻", type="primary", use_container_width=True):
        with st.spinner("正在从中国政府网、国务院政策库和国家网信办抓取真实政策与新闻..."):
            result = run_policy_digest_fetch()
        if result["code"] == 0:
            st.success("政策与新闻抓取完成，页面已刷新为最新结果。")
            st.rerun()
        else:
            st.error("抓取失败。")
            if result["stderr"].strip():
                st.code(result["stderr"])
    if daily_digest_path.exists():
        action_col2.download_button(
            "下载今日日报",
            data=daily_digest_path.read_bytes(),
            file_name=daily_digest_path.name,
            mime="text/markdown",
            use_container_width=True,
        )
    latest_dir = snapshot["latest_dir"]
    action_col3.caption(
        f"数据目录：`{latest_dir}` | 最近抓取：`{summary.get('run_at', '尚未抓取')}`"
    )

    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
    metric_col1.metric("核心政策", len(core_policies))
    metric_col2.metric("全部政策", len(all_policies))
    metric_col3.metric("今日新增", len(daily_updates))
    metric_col4.metric("最近更新", str(summary.get("run_at", "尚未抓取"))[:16] or "尚未抓取")

    if stage1_rows:
        st.markdown("### 政策与论文联动提醒")
        if gap_insights["gap_rows"]:
            gap_dl_col1, gap_dl_col2, gap_dl_col3 = st.columns(3)
            gap_csv = Path(gap_bundle["csv"])
            gap_xlsx = Path(gap_bundle["xlsx"])
            gap_md = Path(gap_bundle["md"])
            if gap_csv.exists():
                gap_dl_col1.download_button("下载缺口分析 CSV", data=gap_csv.read_bytes(), file_name=gap_csv.name, mime="text/csv", use_container_width=True)
            if gap_xlsx.exists():
                gap_dl_col2.download_button(
                    "下载缺口分析 Excel",
                    data=gap_xlsx.read_bytes(),
                    file_name=gap_xlsx.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            if gap_md.exists():
                gap_dl_col3.download_button("下载缺口分析 Markdown", data=gap_md.read_bytes(), file_name=gap_md.name, mime="text/markdown", use_container_width=True)
            st.dataframe(pd.DataFrame(gap_insights["gap_rows"]), use_container_width=True, hide_index=True)
            st.info("这里已经升级成关系链级提醒：会把政策映射到建议的前因变量、机制/调节变量和结果变量链条，再与你最近一次论文编码结果做覆盖对比。")
            st.markdown("#### 研究命题草案")
            prop_dl_col1, prop_dl_col2, prop_dl_col3 = st.columns(3)
            prop_csv = Path(proposition_bundle["csv"])
            prop_xlsx = Path(proposition_bundle["xlsx"])
            prop_md = Path(proposition_bundle["md"])
            if prop_csv.exists():
                prop_dl_col1.download_button("下载命题草案 CSV", data=prop_csv.read_bytes(), file_name=prop_csv.name, mime="text/csv", use_container_width=True)
            if prop_xlsx.exists():
                prop_dl_col2.download_button(
                    "下载命题草案 Excel",
                    data=prop_xlsx.read_bytes(),
                    file_name=prop_xlsx.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            if prop_md.exists():
                prop_dl_col3.download_button("下载命题草案 Markdown", data=prop_md.read_bytes(), file_name=prop_md.name, mime="text/markdown", use_container_width=True)
            st.dataframe(pd.DataFrame(proposition_drafts["rows"]), use_container_width=True, hide_index=True)
            st.caption("这些命题草案会根据政策主题、建议关系链和当前论文缺口自动转写成可继续打磨的论文命题初稿。")
            st.markdown("#### 研究假设草案")
            hyp_dl_col1, hyp_dl_col2, hyp_dl_col3 = st.columns(3)
            hyp_csv = Path(hypothesis_bundle["csv"])
            hyp_xlsx = Path(hypothesis_bundle["xlsx"])
            hyp_md = Path(hypothesis_bundle["md"])
            if hyp_csv.exists():
                hyp_dl_col1.download_button("下载假设草案 CSV", data=hyp_csv.read_bytes(), file_name=hyp_csv.name, mime="text/csv", use_container_width=True)
            if hyp_xlsx.exists():
                hyp_dl_col2.download_button(
                    "下载假设草案 Excel",
                    data=hyp_xlsx.read_bytes(),
                    file_name=hyp_xlsx.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            if hyp_md.exists():
                hyp_dl_col3.download_button("下载假设草案 Markdown", data=hyp_md.read_bytes(), file_name=hyp_md.name, mime="text/markdown", use_container_width=True)
            st.dataframe(pd.DataFrame(hypothesis_drafts["rows"]), use_container_width=True, hide_index=True)
            st.caption("这里会把同一条政策关系链自动改写成更适合实证论文的 H1 / H2 / H3 假设表述。")
        else:
            st.success("当前最新政策主题与你最近一次论文编码结果的主要概念和变量提取没有出现明显新增缺口。")
    else:
        st.info("还没有读取到最近一次论文编码结果，所以暂时无法生成“政策与论文联动提醒”。先跑一版论文编码工作台后，这里会自动联动。")

    info_col1, info_col2 = st.columns([1.45, 1])
    with info_col1:
        st.markdown("### 政策分层")
        layer_col1, layer_col2 = st.columns(2)
        layer_col1.markdown(
            """
<div class="policy-card">
  <div class="policy-chip">核心政策</div>
  <h4 style="margin:0 0 8px 0;color:#0f172a;">国家战略与关键监管</h4>
  <p style="margin:0;color:#334155;line-height:1.75;">
    优先收录国家级顶层规划、关键治理规则、重大场景应用政策和核心制度安排。
  </p>
</div>
            """,
            unsafe_allow_html=True,
        )
        layer_col2.markdown(
            """
<div class="policy-card">
  <div class="policy-chip">全部政策</div>
  <h4 style="margin:0 0 8px 0;color:#0f172a;">历年相关政策全景</h4>
  <p style="margin:0;color:#334155;line-height:1.75;">
    覆盖人工智能、生成式 AI、智能制造、算力、数据要素、数字经济等关联政策。
  </p>
</div>
            """,
            unsafe_allow_html=True,
        )
    with info_col2:
        st.markdown("### 每日自动抓取")
        st.markdown(
            """
<div class="policy-card">
  <div class="policy-chip">自动化状态</div>
  <h4 style="margin:0 0 8px 0;color:#0f172a;">政策 / 新闻双通道</h4>
  <p style="margin:0 0 10px 0;color:#334155;line-height:1.75;">
    当前已经接入三类真实来源：
    1. 国务院政策库历史检索
    2. 中国政府网政策 / 解读页面
    3. 国家网信办首页相关政策与新闻
  </p>
  <p style="margin:0;color:#475569;font-size:13px;">
    每次抓取都会写入状态文件，自动做去重，并生成当日日报，方便后续接每日定时运行。
  </p>
</div>
            """,
            unsafe_allow_html=True,
        )

    tab_core, tab_all, tab_news, tab_manual = st.tabs(["核心政策", "全部政策", "每日抓取预览", "人工校正"])

    with tab_core:
        st.markdown("### 核心政策清单")
        st.caption("这里突出国家级、纲领性、治理型和场景型关键政策，适合快速建立研究框架。")
        render_policy_cards(core_policies or POLICY_CORE_ITEMS, mode="policy")

    with tab_all:
        st.markdown("### 全部政策清单")
        st.caption("这里先做历年相关政策总览 UI，后续会按年份、发布机构、政策类型和关键词做真实筛选。")
        years = sorted({item.get("年份", "") or str(item.get("published_at", ""))[:4] for item in all_policies if item.get("年份", "") or item.get("published_at", "")})
        issuers = sorted({item.get("发布机构", "") or item.get("issuing_body", "") for item in all_policies if item.get("发布机构", "") or item.get("issuing_body", "")})
        types = sorted({item.get("类型", "") or item.get("category", "") for item in all_policies if item.get("类型", "") or item.get("category", "")})
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        selected_year = filter_col1.selectbox("年份筛选", options=["全部"] + years, index=0)
        selected_issuer = filter_col2.selectbox("发布机构", options=["全部"] + issuers, index=0)
        selected_type = filter_col3.selectbox("政策类型", options=["全部"] + types, index=0)
        filtered_policies = []
        for item in all_policies:
            year_value = item.get("年份", "") or str(item.get("published_at", ""))[:4]
            issuer_value = item.get("发布机构", "") or item.get("issuing_body", "")
            type_value = item.get("类型", "") or item.get("category", "")
            if selected_year != "全部" and year_value != selected_year:
                continue
            if selected_issuer != "全部" and issuer_value != selected_issuer:
                continue
            if selected_type != "全部" and type_value != selected_type:
                continue
            filtered_policies.append(item)
        render_policy_cards(filtered_policies or all_policies or POLICY_ALL_ITEMS, mode="policy")

    with tab_news:
        st.markdown("### 每日抓取预览")
        st.caption("这里已经开始读取真实抓取结果。后续接每日自动任务后，会自动更新当天新增政策、官方解读和相关新闻。")
        news_col1, news_col2 = st.columns([1.2, 1])
        news_col1.dataframe(
            pd.DataFrame(
                [
                    {"任务": "政策增量抓取", "状态": "已接入", "频率": "支持每日运行", "来源": "Gov.cn / 国务院政策库"},
                    {"任务": "新闻与解读抓取", "状态": "已接入", "频率": "支持每日运行", "来源": "中国政府网 / 国家网信办"},
                ]
            ),
            hide_index=True,
            use_container_width=True,
        )
        news_col2.markdown(
            """
<div class="policy-card">
  <div class="policy-chip">页面预留</div>
  <h4 style="margin:0 0 8px 0;color:#0f172a;">每日逻辑说明</h4>
  <p style="margin:0;color:#334155;line-height:1.75;">
    当前抓取脚本会自动保存状态文件、去重键和当日日报。后续只需要把这个脚本挂到每日定时任务上，就会形成稳定的每日政策监测流。
  </p>
</div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("#### 今日新增")
        render_policy_cards(daily_updates or news_updates or POLICY_NEWS_ITEMS, mode="news")
        st.markdown("#### 最新新闻 / 官方解读")
        render_policy_cards(news_updates or POLICY_NEWS_ITEMS, mode="news")

    with tab_manual:
        render_policy_correction_panel(snapshot)


def inject_auto_coding_styles() -> None:
    st.markdown(
        """
<style>
.auto-coding-shell {
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 24px;
  background: linear-gradient(180deg, #fcfffe 0%, #f4fbf9 100%);
  padding: 22px 24px 24px 24px;
  margin-bottom: 18px;
}
.auto-coding-kicker {
  color: #0f766e;
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  margin-bottom: 10px;
}
.auto-coding-title {
  font-size: 31px;
  line-height: 1.15;
  font-weight: 800;
  color: #102a43;
  margin: 0 0 8px 0;
}
.auto-coding-subtitle {
  color: #486581;
  font-size: 15px;
  line-height: 1.7;
  margin: 0;
}
.auto-stepper {
  display: flex;
  gap: 10px;
  margin: 14px 0 22px 0;
  flex-wrap: wrap;
}
.auto-step {
  position: relative;
  display: inline-flex;
  align-items: center;
  gap: 10px;
  text-decoration: none !important;
  background: #dcefe9;
  color: #1f2933 !important;
  padding: 14px 26px 14px 22px;
  min-width: 220px;
  clip-path: polygon(0 0, calc(100% - 16px) 0, 100% 50%, calc(100% - 16px) 100%, 0 100%, 14px 50%);
  transition: transform .15s ease, filter .15s ease;
}
.auto-step:hover {
  transform: translateY(-1px);
  filter: brightness(0.98);
}
.auto-step.is-active {
  background: linear-gradient(135deg, #0f766e 0%, #155e75 100%);
  color: white !important;
}
.auto-step-index {
  width: 28px;
  height: 28px;
  border-radius: 999px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  background: rgba(255,255,255,0.22);
  font-size: 12px;
  font-weight: 700;
  flex: 0 0 auto;
}
.auto-step:not(.is-active) .auto-step-index {
  background: rgba(15,118,110,0.12);
  color: #0f766e;
}
.auto-step-copy {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.auto-step-label {
  font-size: 15px;
  font-weight: 700;
  line-height: 1.2;
}
.auto-step-meta {
  font-size: 12px;
  opacity: 0.82;
  line-height: 1.2;
}
.auto-stage-card {
  border: 1px solid rgba(15, 118, 110, 0.12);
  border-radius: 18px;
  padding: 18px;
  background: white;
  min-height: 128px;
}
.auto-stage-tag {
  display: inline-flex;
  padding: 6px 10px;
  border-radius: 999px;
  background: #e6fffb;
  color: #0f766e;
  font-size: 12px;
  font-weight: 700;
  margin-bottom: 10px;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def current_auto_coding_step() -> str:
    step = st.query_params.get("auto_coding_step", "step1")
    return step if step in {"step1", "step2", "step3"} else "step1"


def render_auto_coding_stepper(step: str) -> None:
    steps = [
        ("step1", "01", "文献信息提取", "论文信息、变量、Prompt"),
        ("step2", "02", "编码深化分析", "开放编码、主轴编码、命题"),
        ("step3", "03", "结果汇总输出", "表格、报告、批注与导出"),
    ]
    html = ['<div class="auto-stepper">']
    for key, index, label, meta in steps:
        active_class = " is-active" if key == step else ""
        html.append(
            f'<a class="auto-step{active_class}" href="?page=文献自动化编码&auto_coding_step={key}">'
            f'<span class="auto-step-index">{index}</span>'
            f'<span class="auto-step-copy"><span class="auto-step-label">{label}</span>'
            f'<span class="auto-step-meta">{meta}</span></span></a>'
        )
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def build_placeholder_stage1_table(rows: int = 18) -> pd.DataFrame:
    prompt = "我将按照学术文献解构框架，系统分析这篇关于企业人工智能采纳的实证研究论文。"
    data = []
    for index in range(1, rows + 1):
        data.append(
            {
                "序号": index,
                "附件预览": "",
                "附件": "",
                "查看原文": "",
                "主要概念": "",
                "主要观点": "",
                "变量筛选prompt": prompt,
            }
        )
    return pd.DataFrame(data)


def store_auto_coding_stage1_results(
    *,
    run_dir: Path,
    prepared: dict[str, Any],
    stage1_df: pd.DataFrame,
    output_paths: dict[str, str],
) -> None:
    stage1_df = enrich_stage1_dataframe(stage1_df)
    st.session_state["auto_coding_stage1_run"] = str(run_dir)
    st.session_state["auto_coding_stage1_inventory"] = prepared["inventory_paths"]
    st.session_state["auto_coding_stage1_batch_count"] = len(prepared["batches"])
    st.session_state["auto_coding_stage1_file_count"] = len(prepared["files"])
    st.session_state["auto_coding_stage1_rows"] = stage1_df.to_dict(orient="records")
    st.session_state["auto_coding_stage1_csv"] = output_paths["csv"]
    st.session_state["auto_coding_stage1_xlsx"] = output_paths["xlsx"]


def persist_auto_coding_stage1_rows(dataframe: pd.DataFrame) -> None:
    reordered = enrich_stage1_dataframe(renumber_rows(dataframe))
    st.session_state["auto_coding_stage1_rows"] = reordered.to_dict(orient="records")
    run_dir_raw = st.session_state.get("auto_coding_stage1_run", "")
    if run_dir_raw:
        edited_output = save_stage1_outputs(Path(run_dir_raw), reordered)
        st.session_state["auto_coding_stage1_csv"] = edited_output["csv"]
        st.session_state["auto_coding_stage1_xlsx"] = edited_output["xlsx"]


def move_attachment_row(position: int, delta: int) -> None:
    rows = st.session_state.get("auto_coding_stage1_rows", [])
    if not rows:
        return
    dataframe = enrich_stage1_dataframe(pd.DataFrame(rows))
    attached = dataframe[dataframe["附件"].fillna("").astype(str).str.strip() != ""].copy()
    blanks = dataframe[dataframe["附件"].fillna("").astype(str).str.strip() == ""].copy()
    if attached.empty:
        return
    target = max(0, min(len(attached) - 1, position + delta))
    if target == position:
        return
    rows_list = attached.to_dict(orient="records")
    item = rows_list.pop(position)
    rows_list.insert(target, item)
    persist_auto_coding_stage1_rows(pd.DataFrame(rows_list + blanks.to_dict(orient="records")))


def render_attachment_manager(detail_df: pd.DataFrame) -> None:
    attachment_df = detail_df[detail_df["附件"].fillna("").astype(str).str.strip() != ""].copy()
    if attachment_df.empty:
        return
    st.markdown("### 附件管理")
    st.caption("这里改成稳定可用的排序方式。点击上移或下移后，第一步主表会同步重排并重新编号。")
    total = len(attachment_df)
    for position, (_, row) in enumerate(attachment_df.iterrows()):
        preview_col, meta_col, action_col = st.columns([1.05, 3.2, 1.5])
        preview_path = str(row.get("附件预览", "")).strip()
        if preview_path:
            preview_col.image(preview_path, use_container_width=True)
        else:
            preview_col.caption("无预览")
        meta_col.markdown(f"**{position + 1}. {row['附件']}**")
        meta_col.caption(f"批次：{row.get('批次', '') or '未分配'}")
        meta_col.markdown(f"[查看原文]({row.get('查看原文', '')})")
        up_disabled = position == 0
        down_disabled = position == total - 1
        up_col, down_col = action_col.columns(2)
        if up_col.button("上移", key=f"move-up-{position}", disabled=up_disabled, use_container_width=True):
            move_attachment_row(position, -1)
            st.rerun()
        if down_col.button("下移", key=f"move-down-{position}", disabled=down_disabled, use_container_width=True):
            move_attachment_row(position, 1)
            st.rerun()


def render_document_actions(selected_row: pd.Series) -> None:
    file_path = str(selected_row.get("文件路径", "")).strip()
    if not file_path:
        return
    action_col1, action_col2, action_col3 = st.columns([1, 1, 2])
    if action_col1.button("打开原文", key=f"open-doc-{file_path}"):
        subprocess.Popen(["open", file_path], cwd=APP_ROOT)
    parent_dir = str(Path(file_path).resolve().parent)
    if action_col2.button("打开所在目录", key=f"open-dir-{file_path}"):
        subprocess.Popen(["open", parent_dir], cwd=APP_ROOT)
    action_col3.markdown(f"[在新窗口查看原文]({as_file_uri(file_path)})")


def render_auto_coding_stage1_results() -> None:
    rows = st.session_state.get("auto_coding_stage1_rows", [])
    if not rows:
        return

    stage1_df = enrich_stage1_dataframe(pd.DataFrame(rows))
    st.markdown("### 第一步主表")
    st.caption("这里已经接入真实提取结果。你可以直接在表格里修改主要概念、主要观点和每篇文献的变量筛选 prompt。")
    column_order = ["序号", "附件预览", "附件", "查看原文", "主要概念", "主要观点", "变量筛选prompt"]
    edited_df = st.data_editor(
        stage1_df,
        column_order=column_order,
        hide_index=True,
        use_container_width=True,
        height=720,
        num_rows="dynamic",
        column_config={
            "序号": st.column_config.NumberColumn("序号", disabled=True, width="small"),
            "附件预览": st.column_config.ImageColumn("附件", help="自动生成的附件预览图", width="small"),
            "附件": st.column_config.TextColumn("附件", disabled=True, width="medium"),
            "查看原文": st.column_config.LinkColumn("查看原文", width="small", display_text="打开"),
            "主要概念": st.column_config.TextColumn("主要概念", width="medium"),
            "主要观点": st.column_config.TextColumn("主要观点", width="large"),
            "变量筛选prompt": st.column_config.TextColumn("变量筛选prompt", width="large"),
        },
        key="auto-coding-stage1-editor-live",
    )
    st.session_state["auto_coding_stage1_rows"] = enrich_stage1_dataframe(pd.DataFrame(edited_df)).to_dict(orient="records")
    run_dir_raw = st.session_state.get("auto_coding_stage1_run", "")
    if run_dir_raw:
        edited_output = save_stage1_outputs(Path(run_dir_raw), enrich_stage1_dataframe(pd.DataFrame(edited_df)))
        st.session_state["auto_coding_stage1_csv"] = edited_output["csv"]
        st.session_state["auto_coding_stage1_xlsx"] = edited_output["xlsx"]

    download_col1, download_col2, download_col3 = st.columns([1, 1, 2])
    csv_path = Path(st.session_state.get("auto_coding_stage1_csv", ""))
    xlsx_path = Path(st.session_state.get("auto_coding_stage1_xlsx", ""))
    if csv_path.exists():
        download_col1.download_button("下载第一步主表 CSV", data=csv_path.read_bytes(), file_name=csv_path.name, mime="text/csv")
    if xlsx_path.exists():
        download_col2.download_button(
            "下载第一步主表 Excel",
            data=xlsx_path.read_bytes(),
            file_name=xlsx_path.name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    run_dir_text = st.session_state.get("auto_coding_stage1_run", "")
    if run_dir_text:
        download_col3.caption(f"运行目录：`{run_dir_text}`")

    render_attachment_manager(enrich_stage1_dataframe(pd.DataFrame(edited_df)))

    st.markdown("### 论文详情查看")
    detail_df = enrich_stage1_dataframe(pd.DataFrame(edited_df))
    selectable_df = detail_df[detail_df["附件"].fillna("").astype(str).str.strip() != ""].copy()
    options = [f"{int(row['序号'])}. {row['附件']} | {row['标题']}" for _, row in selectable_df.iterrows()]
    if options:
        selected_option = st.selectbox("选择一篇文献查看详细提取结果", options=options, key="auto-coding-detail-select")
        selected_index = options.index(selected_option)
        selected_row = selectable_df.iloc[selected_index]
        detail_map = {
            "标题": selected_row["标题"],
            "作者": selected_row["作者"],
            "期刊": selected_row["期刊"],
            "年份": selected_row["年份"],
            "样本特征": selected_row["样本特征"],
            "分析方法": selected_row["分析方法"],
            "理论基础": selected_row["理论基础"],
            "自变量": selected_row["自变量"],
            "中介/调节变量": selected_row["中介/调节变量"],
            "因变量/结果变量": selected_row["因变量/结果变量"],
            "控制变量": selected_row["控制变量"],
            "未来研究方向": selected_row["未来研究方向"],
            "未来研究编码": selected_row["未来研究编码"],
            "文件路径": selected_row["文件路径"],
            "批次": selected_row["批次"],
        }
        st.table(pd.DataFrame({"字段": list(detail_map.keys()), "内容": list(detail_map.values())}))
        render_document_actions(selected_row)
        st.markdown("#### 当前行 Prompt")
        st.code(str(selected_row["变量筛选prompt"]))


def literature_auto_coding_panel() -> None:
    inject_auto_coding_styles()
    step = current_auto_coding_step()

    st.markdown(
        """
<div class="auto-coding-shell">
  <div class="auto-coding-kicker">Literature Automation</div>
  <h1 class="auto-coding-title">文献自动化编码</h1>
  <p class="auto-coding-subtitle">
    这个功能会拆成三个部分。当前先把新的交互界面搭起来：
    顶部是可点击的箭头式步骤导航，第一步对应你给的文献信息与变量提取主表。
  </p>
</div>
        """,
        unsafe_allow_html=True,
    )

    render_auto_coding_stepper(step)

    if step == "step1":
        st.markdown("### 第一步：文献信息与变量提取")
        card_col1, card_col2, card_col3 = st.columns(3)
        card_col1.markdown(
            """
<div class="auto-stage-card">
  <div class="auto-stage-tag">当前阶段</div>
  <h4 style="margin:0 0 8px 0;color:#102a43;">基础信息抽取</h4>
  <p style="margin:0;color:#52606d;line-height:1.7;">
    先提取作者、标题、期刊、年份、样本特征、分析方法等基础信息。
  </p>
</div>
            """,
            unsafe_allow_html=True,
        )
        card_col2.markdown(
            """
<div class="auto-stage-card">
  <div class="auto-stage-tag">当前阶段</div>
  <h4 style="margin:0 0 8px 0;color:#102a43;">变量筛选</h4>
  <p style="margin:0;color:#52606d;line-height:1.7;">
    把自变量、中介/调节变量、因变量、控制变量放进统一表格，方便后续编码。
  </p>
</div>
            """,
            unsafe_allow_html=True,
        )
        card_col3.markdown(
            """
<div class="auto-stage-card">
  <div class="auto-stage-tag">当前阶段</div>
  <h4 style="margin:0 0 8px 0;color:#102a43;">Prompt 准备</h4>
  <p style="margin:0;color:#52606d;line-height:1.7;">
    最后一列保留每篇文献的默认 Prompt，后面你可以继续修改或替换成自己的版本。
  </p>
</div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("### 导入与生成")
        with st.form("auto-coding-stage1-form"):
            project_name = st.text_input("项目名称", value="文献自动化编码")
            source_inputs = source_import_block("auto-coding", allowed_hint="pdf/docx/txt/md")
            include_policy_context = st.checkbox(
                "同步挂接最新政策 / 新闻补充包",
                value=policy_snapshot_available(),
                help="不会把政策当论文来提取，但会在本次运行目录里生成政策与新闻补充材料，供后续编码深化分析使用。",
            )
            prompt_template = st.text_area(
                "默认变量筛选 Prompt 模板",
                value=DEFAULT_VARIABLE_PROMPT_TEMPLATE,
                height=260,
                help="这一列会自动带到每篇文献里，后面你可以逐行修改。",
            )
            split_col1, split_col2, split_col3 = st.columns(3)
            max_files = split_col1.slider("每批最多文件数", min_value=5, max_value=80, value=20, step=1, key="auto-stage1-max-files")
            max_pages = split_col2.slider("每批最多估算页数", min_value=100, max_value=1200, value=450, step=50, key="auto-stage1-max-pages")
            max_size = split_col3.slider("每批最大体积（MB）", min_value=20, max_value=500, value=120, step=10, key="auto-stage1-max-size")
            submitted = st.form_submit_button("生成第一步提取表", type="primary")

        if submitted:
            run_dir = RUNS_ROOT / f"auto_coding_stage1_{timestamp_id()}"
            policy_bundle = build_policy_context_bundle(run_dir) if include_policy_context and policy_snapshot_available() else {}
            prepared = prepare_batches(
                run_dir=run_dir,
                allowed_suffixes=PAPER_CODING_SUFFIXES,
                selected_desktop_names=source_inputs["selected_desktop"],
                custom_paths_text=source_inputs["custom_paths"],
                uploaded_files=source_inputs["uploaded_files"],
                max_files_per_batch=max_files,
                max_pages_per_batch=max_pages,
                max_size_mb_per_batch=max_size,
            )
            if not prepared["files"]:
                st.warning("没有扫描到可用于文献自动化编码的文件。请检查文件夹、路径或上传内容。")
            else:
                with st.spinner("正在提取论文基础信息、变量和默认 Prompt，请稍候..."):
                    stage1_df = build_stage1_dataframe(run_dir, prepared["files"], prompt_template=prompt_template)
                batch_lookup: dict[str, str] = {}
                for batch in prepared["batches"]:
                    for file_path in batch.file_paths:
                        batch_lookup[file_path] = batch.batch_id
                stage1_df["批次"] = stage1_df["文件路径"].map(batch_lookup).fillna("")
                stage1_df = enrich_stage1_dataframe(stage1_df)
                output_paths = save_stage1_outputs(run_dir, stage1_df)
                store_auto_coding_stage1_results(
                    run_dir=run_dir,
                    prepared=prepared,
                    stage1_df=stage1_df,
                    output_paths=output_paths,
                )
                st.success(f"{project_name} 的第一步提取表已生成。")
                if policy_bundle:
                    st.info(f"本次运行已挂接政策 / 新闻补充包：`{policy_bundle['combined_md']}`")

        if st.session_state.get("auto_coding_stage1_rows"):
            inventory_paths = st.session_state.get("auto_coding_stage1_inventory", {})
            st.markdown("### 批次预览")
            metric_col1, metric_col2, metric_col3 = st.columns(3)
            metric_col1.metric("文件数", int(st.session_state.get("auto_coding_stage1_file_count", 0)))
            metric_col2.metric("批次数", int(st.session_state.get("auto_coding_stage1_batch_count", 0)))
            batch_csv_raw = inventory_paths.get("batch_csv", "") if inventory_paths else ""
            inventory_csv_raw = inventory_paths.get("inventory_csv", "") if inventory_paths else ""
            batch_csv = Path(batch_csv_raw) if batch_csv_raw else None
            inventory_csv = Path(inventory_csv_raw) if inventory_csv_raw else None
            if batch_csv and batch_csv.exists():
                batch_df = pd.read_csv(batch_csv)
                total_pages = int(batch_df["total_estimated_pages"].sum()) if not batch_df.empty else 0
            else:
                total_pages = 0
            metric_col3.metric("估算总页数", total_pages)
            if inventory_csv and inventory_csv.exists():
                with st.expander("查看文件清单", expanded=False):
                    st.dataframe(pd.read_csv(inventory_csv).head(60), use_container_width=True)
            render_auto_coding_stage1_results()
        else:
            st.markdown("### 主表预览")
            uploaded_seed = source_inputs["uploaded_files"] if source_inputs["uploaded_files"] else []
            if uploaded_seed:
                st.caption("你刚上传的附件已经按顺序放进“附件”列里了，每个附件一行；表格也可以继续往下新增。")
            else:
                st.caption("这是第一步的目标表结构。你导入文献并点击生成后，这里会自动变成真实提取结果。")
            preview_root = RUNTIME_ROOT / "seed_preview" / "auto_coding"
            preview_df = build_attachment_seed_table(uploaded_seed, preview_root) if uploaded_seed else build_placeholder_stage1_table()
            st.data_editor(
                preview_df,
                hide_index=True,
                use_container_width=True,
                height=520,
                num_rows="dynamic",
                column_config={
                    "序号": st.column_config.NumberColumn("序号", disabled=True, width="small"),
                    "附件预览": st.column_config.ImageColumn("附件", width="small"),
                    "附件": st.column_config.TextColumn("附件", width="medium"),
                    "查看原文": st.column_config.LinkColumn("查看原文", width="small", display_text="打开"),
                    "主要概念": st.column_config.TextColumn("主要概念", width="medium"),
                    "主要观点": st.column_config.TextColumn("主要观点", width="large"),
                    "变量筛选prompt": st.column_config.TextColumn("变量筛选prompt", width="large"),
                },
                key="auto-coding-stage1-ui",
            )
            st.info("先导入桌面文件夹、手动路径或上传文件，然后点击“生成第一步提取表”。")

    elif step == "step2":
        st.markdown("### 第二步：编码深化分析")
        st.caption("这一部分先搭结构，不接功能。后面可以放开放编码、主轴编码、研究命题和未来研究方向。")
        cols = st.columns(3)
        cards = [
            ("开放编码", "逐句抽取原始概念、变量线索与证据句。"),
            ("主轴编码", "把前因、机制、结果、边界条件串成关系链。"),
            ("命题整理", "汇总假设、命题、显著与不显著关系。"),
        ]
        for column, (title, desc) in zip(cols, cards):
            column.markdown(
                f"""
<div class="auto-stage-card">
  <div class="auto-stage-tag">预留步骤</div>
  <h4 style="margin:0 0 8px 0;color:#102a43;">{title}</h4>
  <p style="margin:0;color:#52606d;line-height:1.7;">{desc}</p>
</div>
                """,
                unsafe_allow_html=True,
            )
        st.dataframe(
            pd.DataFrame(
                [
                    {"模块": "开放编码", "状态": "待接入", "说明": "后续接你的第二步流程"},
                    {"模块": "主轴编码", "状态": "待接入", "说明": "后续接变量关系链和命题整理"},
                    {"模块": "未来研究编码", "状态": "待接入", "说明": "后续接研究方向和批注提炼"},
                ]
            ),
            hide_index=True,
            use_container_width=True,
        )

    else:
        st.markdown("### 第三步：结果汇总输出")
        st.caption("这一部分也先做 UI 结构，后面再接导出、汇总、批注和最终报告。")
        cols = st.columns(3)
        cards = [
            ("总表汇总", "把多篇文献整理成统一总表。"),
            ("报告输出", "生成文献解析报告、编码批注和汇总结论。"),
            ("导出交付", "导出 Excel、Markdown、后续可扩展为 Word。"),
        ]
        for column, (title, desc) in zip(cols, cards):
            column.markdown(
                f"""
<div class="auto-stage-card">
  <div class="auto-stage-tag">预留步骤</div>
  <h4 style="margin:0 0 8px 0;color:#102a43;">{title}</h4>
  <p style="margin:0;color:#52606d;line-height:1.7;">{desc}</p>
</div>
                """,
                unsafe_allow_html=True,
            )
        st.dataframe(
            pd.DataFrame(
                [
                    {"输出项": "文献总表", "格式": "表格", "状态": "待接入"},
                    {"输出项": "文献解析报告", "格式": "Markdown / Word", "状态": "待接入"},
                    {"输出项": "编码批注", "格式": "附表", "状态": "待接入"},
                ]
            ),
            hide_index=True,
            use_container_width=True,
        )


def desktop_directory_map() -> dict[str, Path]:
    return {path.name: path for path in list_desktop_directories()}


def resolve_source_paths(
    *,
    selected_desktop_names: list[str],
    custom_paths_text: str,
    uploaded_files: list[Any],
    upload_dir: Path,
) -> list[Path]:
    desktop_map = desktop_directory_map()
    collected = [str(desktop_map[name]) for name in selected_desktop_names if name in desktop_map]
    collected.extend([line.strip() for line in custom_paths_text.splitlines() if line.strip()])
    saved = save_uploaded_files(uploaded_files or [], upload_dir)
    if saved:
        collected.append(str(upload_dir))
    return normalize_input_paths(collected)


def prepare_batches(
    *,
    run_dir: Path,
    allowed_suffixes: set[str],
    selected_desktop_names: list[str],
    custom_paths_text: str,
    uploaded_files: list[Any],
    max_files_per_batch: int,
    max_pages_per_batch: int,
    max_size_mb_per_batch: int,
) -> dict[str, Any]:
    upload_dir = run_dir / "imported_sources"
    input_paths = resolve_source_paths(
        selected_desktop_names=selected_desktop_names,
        custom_paths_text=custom_paths_text,
        uploaded_files=uploaded_files,
        upload_dir=upload_dir,
    )
    files = scan_source_files(input_paths, allowed_suffixes=allowed_suffixes)
    files = reorder_files_by_upload_order(files, uploaded_files)
    batches = split_into_batches(
        files,
        max_files_per_batch=max_files_per_batch,
        max_pages_per_batch=max_pages_per_batch,
        max_size_mb_per_batch=max_size_mb_per_batch,
    )
    inventory_paths = write_inventory(run_dir, files, batches)
    batch_dirs = build_batch_symlink_folders(run_dir, batches)
    return {
        "input_paths": input_paths,
        "files": files,
        "batches": batches,
        "inventory_paths": inventory_paths,
        "batch_dirs": batch_dirs,
    }


def render_batch_preview(prepared: dict[str, Any], *, title: str) -> None:
    files = prepared["files"]
    batches = prepared["batches"]
    st.markdown(f"### {title}")
    col1, col2, col3 = st.columns(3)
    col1.metric("文件数", len(files))
    col2.metric("批次数", len(batches))
    total_pages = sum(item.estimated_pages for item in files)
    col3.metric("估算总页数", total_pages)

    inventory_csv = Path(prepared["inventory_paths"]["inventory_csv"])
    batch_csv = Path(prepared["inventory_paths"]["batch_csv"])
    if inventory_csv.exists():
        st.dataframe(pd.read_csv(inventory_csv).head(50), use_container_width=True)
    render_download(inventory_csv, "下载文件清单 CSV", "text/csv")
    render_download(batch_csv, "下载批次汇总 CSV", "text/csv")
    render_download(Path(prepared["inventory_paths"]["batch_json"]), "下载批次清单 JSON", "application/json")


def merge_batch_csvs(batch_result_csvs: list[Path], merged_csv: Path, merged_xlsx: Path) -> tuple[str, str]:
    frames = []
    for csv_path in batch_result_csvs:
        if csv_path.exists():
            frames.append(pd.read_csv(csv_path))
    if not frames:
        return "", ""
    merged = pd.concat(frames, ignore_index=True)
    merged.to_csv(merged_csv, index=False)
    merged.to_excel(merged_xlsx, index=False)
    return str(merged_csv), str(merged_xlsx)


def run_paper_coding_batches(
    *,
    run_dir: Path,
    prepared: dict[str, Any],
    project_name: str,
    queries: list[str],
    covered_topics: list[str],
    baseline_paths: list[str],
    enable_agent: bool,
    enable_llm: bool,
    api_url: str,
    model_name: str,
    api_key: str,
) -> dict[str, Any]:
    batch_root = run_dir / "paper_coding_batches"
    batch_root.mkdir(parents=True, exist_ok=True)
    summaries = []
    batch_csvs: list[Path] = []

    for batch, batch_dir in zip(prepared["batches"], prepared["batch_dirs"]):
        batch_run_dir = batch_root / batch.batch_id
        config = base_grounded_config(batch_run_dir)
        config["project_name"] = f"{project_name}-{batch.batch_id}"
        config["queries"] = queries or [project_name]
        config["covered_topics"] = covered_topics
        config["baseline_paths"] = baseline_paths
        config["local_library_paths"] = [batch_dir]
        config["max_local_papers"] = max(batch.file_count, 20)
        config["agent"]["enabled"] = enable_agent
        if enable_llm and api_url and model_name and api_key:
            config["assistant"]["enabled"] = True
            config["assistant"]["api_url"] = api_url
            config["assistant"]["model"] = model_name

        config_path = batch_run_dir / "grounded_config.json"
        write_json(config_path, config)
        env = {"GROUNDED_AGENT_API_KEY": api_key} if enable_llm else {}
        code, stdout, stderr = run_command(
            [PYTHON_BIN, "grounded_daily_monitor.py", "--config", str(config_path)],
            env,
        )
        summary = extract_last_json_block(stdout)
        summary_row = {
            "batch_id": batch.batch_id,
            "file_count": batch.file_count,
            "exit_code": code,
            "results_csv": summary.get("results_csv", ""),
            "results_xlsx": summary.get("results_xlsx", ""),
            "daily_report": summary.get("daily_report", ""),
            "stderr": stderr.strip()[:2000],
        }
        summaries.append(summary_row)
        if summary_row["results_csv"]:
            batch_csvs.append(Path(summary_row["results_csv"]))

    batch_run_csv = run_dir / "paper_coding_batch_runs.csv"
    pd.DataFrame(summaries).to_csv(batch_run_csv, index=False)
    merged_dir = run_dir / "paper_coding_merged"
    merged_dir.mkdir(parents=True, exist_ok=True)
    merged_csv, merged_xlsx = merge_batch_csvs(
        batch_csvs,
        merged_dir / "merged_literature_table.csv",
        merged_dir / "merged_literature_table.xlsx",
    )
    return {
        "batch_run_csv": str(batch_run_csv),
        "merged_csv": merged_csv,
        "merged_xlsx": merged_xlsx,
    }


def render_followup_tools() -> None:
    latest_config = st.session_state.get("latest_paper_coding_config", "")
    latest_run = st.session_state.get("latest_paper_coding_run", "")
    if not latest_config or not Path(latest_config).exists():
        return

    st.markdown("---")
    st.subheader("基于论文编码结果继续工作")
    question = st.text_area("继续提问", value="当前文献里最常见的前因、机制和边界条件是什么？", height=80, key="followup-question")
    report_topic = st.text_area("生成轻量主题报告", value="创业研究中的人工智能采纳与组织能力", height=80, key="followup-report")
    col1, col2 = st.columns(2)
    ask_clicked = col1.button("基于编码结果回答问题", key="followup-ask")
    report_clicked = col2.button("基于编码结果生成主题报告", key="followup-generate")
    env = {"GROUNDED_AGENT_API_KEY": st.session_state.get("grounded_api_key", "")}

    if ask_clicked and question.strip():
        with st.spinner("正在生成问答..."):
            code, stdout, stderr = run_command(
                [PYTHON_BIN, "grounded_daily_monitor.py", "--config", latest_config, "--skip-monitor", "--ask", question.strip()],
                env,
            )
        if code == 0:
            summary = extract_last_json_block(stdout)
            qa_path = Path(summary.get("qa_answer", ""))
            render_markdown_file(qa_path, "问答结果")
            render_download(qa_path, "下载问答 Markdown", "text/markdown")
        elif stderr.strip():
            st.error(stderr)

    if report_clicked and report_topic.strip():
        with st.spinner("正在生成主题报告..."):
            code, stdout, stderr = run_command(
                [PYTHON_BIN, "grounded_daily_monitor.py", "--config", latest_config, "--skip-monitor", "--generate-report", report_topic.strip()],
                env,
            )
        if code == 0:
            summary = extract_last_json_block(stdout)
            report_path = Path(summary.get("industry_report", ""))
            render_markdown_file(report_path, "主题报告")
            render_download(report_path, "下载主题报告 Markdown", "text/markdown")
        elif stderr.strip():
            st.error(stderr)

    if latest_run:
        st.caption(f"最近一次论文编码运行目录：`{latest_run}`")


def source_import_block(prefix: str, *, allowed_hint: str) -> dict[str, Any]:
    desktop_names = list(desktop_directory_map().keys())
    st.markdown("#### 资料导入")
    selected_desktop = st.multiselect(
        "从桌面常用文件夹中选择",
        options=desktop_names,
        default=[],
        key=f"{prefix}-desktop",
        help="会自动扫描你桌面上选中的文件夹。",
    )
    custom_paths = st.text_area(
        "或手动填写路径",
        value="",
        height=80,
        key=f"{prefix}-custom-paths",
        help="每行一个绝对路径，可以填文件夹或单个文件。",
    )
    uploaded_files = st.file_uploader(
        f"或直接上传文件（支持：{allowed_hint}）",
        accept_multiple_files=True,
        key=f"{prefix}-uploads",
    )
    return {
        "selected_desktop": selected_desktop,
        "custom_paths": custom_paths,
        "uploaded_files": uploaded_files or [],
    }


def paper_coding_panel() -> None:
    st.subheader("论文编码工作台")
    st.caption("这个工作台已经拆成三步。当前先把第一步做实：批量导入文献，抽取论文信息、变量和 prompt 清单。")

    with st.form("paper-coding-form"):
        project_name = st.text_input("项目名称", value="论文编码工作台")
        queries_text = st.text_area(
            "研究主题词",
            value="创业 即兴行为 扎根理论\n创业 资源视角 扎根理论",
            height=90,
            help="每行一条，作为编码聚焦词。",
        )
        covered_text = st.text_area("已覆盖主题", value="前因\n结果\n边界条件", height=90)
        baseline_files = st.file_uploader(
            "上传你的论文或基准文献",
            type=["pdf", "txt", "md", "docx"],
            accept_multiple_files=True,
            key="paper-baseline",
        )
        include_policy_context = st.checkbox(
            "把最新政策 / 新闻补充包接入本次论文编码工作流",
            value=policy_snapshot_available(),
            help="会在运行目录中生成政策与新闻补充材料，供后续文献编码、研究缺口判断和补充写作方向使用。",
        )
        source_inputs = source_import_block("paper", allowed_hint="pdf/docx/txt/md")
        prompt_template = st.text_area(
            "默认变量筛选 Prompt 模板",
            value=DEFAULT_VARIABLE_PROMPT_TEMPLATE,
            height=320,
            help="这会作为最后一列的默认 prompt，后面你可以逐行修改。",
        )

        st.markdown("#### 自动分批规则")
        max_files = st.slider("每批最多文件数", min_value=5, max_value=80, value=20, step=1)
        max_pages = st.slider("每批最多估算页数", min_value=100, max_value=1200, value=450, step=50)
        max_size = st.slider("每批最大体积（MB）", min_value=20, max_value=500, value=120, step=10)
        enable_agent = st.checkbox("启用内置研究 agent 轨迹", value=True)

        st.markdown("#### 可选：模型增强")
        enable_llm = st.checkbox("启用 OpenAI 兼容接口增强编码与后续问答", value=False, key="paper-enable-llm")
        api_url = st.text_input("API 地址", value="", placeholder="https://your-api.example.com/v1", key="paper-api-url")
        model_name = st.text_input("模型名称", value="", placeholder="gpt-4o-mini", key="paper-model-name")
        api_key = st.text_input("API Key", value="", type="password", key="paper-api-key")

        submitted = st.form_submit_button("生成第一步提取表", type="primary")

    if not submitted:
        return

    run_dir = RUNS_ROOT / f"paper_coding_{timestamp_id()}"
    baseline_dir = run_dir / "baseline_files"
    baseline_paths = save_uploaded_files(baseline_files or [], baseline_dir)
    policy_bundle = build_policy_context_bundle(run_dir) if include_policy_context and policy_snapshot_available() else {}
    if policy_bundle:
        baseline_paths.append(policy_bundle["combined_md"])

    prepared = prepare_batches(
        run_dir=run_dir,
        allowed_suffixes=PAPER_CODING_SUFFIXES,
        selected_desktop_names=source_inputs["selected_desktop"],
        custom_paths_text=source_inputs["custom_paths"],
        uploaded_files=source_inputs["uploaded_files"],
        max_files_per_batch=max_files,
        max_pages_per_batch=max_pages,
        max_size_mb_per_batch=max_size,
    )
    if not prepared["files"]:
        st.warning("没有扫描到可用于论文编码的文件。请检查文件夹、路径或上传内容。")
        return

    st.session_state["latest_paper_coding_run"] = str(run_dir)
    if policy_bundle:
        st.session_state["latest_policy_context_dir"] = policy_bundle["context_dir"]
    st.session_state["grounded_api_key"] = api_key.strip() if enable_llm else ""
    render_batch_preview(prepared, title="第一步：批次拆分预览")
    if policy_bundle:
        st.info(f"本次论文编码已挂接政策 / 新闻补充包：`{policy_bundle['combined_md']}`")

    with st.spinner("正在提取论文信息、变量与默认 prompt，请稍候..."):
        stage1_df = build_stage1_dataframe(run_dir, prepared["files"], prompt_template=prompt_template)
    batch_lookup: dict[str, str] = {}
    for batch in prepared["batches"]:
        for file_path in batch.file_paths:
            batch_lookup[file_path] = batch.batch_id
    stage1_df["批次"] = stage1_df["文件路径"].map(batch_lookup).fillna("")
    output_paths = save_stage1_outputs(run_dir, stage1_df)
    st.session_state["latest_paper_stage1_csv"] = output_paths["csv"]
    st.session_state["latest_paper_stage1_xlsx"] = output_paths["xlsx"]

    step1_tab, step2_tab, step3_tab = st.tabs(
        ["第一步：论文信息与变量提取", "第二步：待接入你的流程", "第三步：待接入你的流程"]
    )

    with step1_tab:
        st.markdown("### 第一步主表")
        st.caption("这一页按你给的表格逻辑组织：序号、附件、主要概念、主要观点，以及最后一列可编辑 prompt。")
        column_order = ["序号", "附件预览", "附件", "主要概念", "主要观点", "变量筛选prompt"]
        editable_df = st.data_editor(
            stage1_df,
            column_order=column_order,
            hide_index=True,
            use_container_width=True,
            height=680,
            num_rows="dynamic",
            column_config={
                "序号": st.column_config.NumberColumn("序号", disabled=True, width="small"),
                "附件预览": st.column_config.ImageColumn("附件", help="自动生成的附件预览图", width="small"),
                "附件": st.column_config.TextColumn("附件", disabled=True, width="medium"),
                "主要概念": st.column_config.TextColumn("主要概念", width="medium"),
                "主要观点": st.column_config.TextColumn("主要观点", width="large"),
                "变量筛选prompt": st.column_config.TextColumn("变量筛选prompt", width="large"),
            },
            key="paper-stage1-editor",
        )
        edited_output = save_stage1_outputs(run_dir, pd.DataFrame(editable_df))
        render_download(Path(edited_output["csv"]), "下载第一步主表 CSV", "text/csv")
        render_download(
            Path(edited_output["xlsx"]),
            "下载第一步主表 Excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.markdown("### 论文详情查看")
        options = [
            f"{int(row['序号'])}. {row['附件']} | {row['标题']}"
            for _, row in pd.DataFrame(editable_df).iterrows()
        ]
        selected_option = st.selectbox("选择一篇文献查看详细提取结果", options=options)
        selected_index = options.index(selected_option)
        selected_row = pd.DataFrame(editable_df).iloc[selected_index]
        detail_map = {
            "标题": selected_row["标题"],
            "作者": selected_row["作者"],
            "期刊": selected_row["期刊"],
            "年份": selected_row["年份"],
            "样本特征": selected_row["样本特征"],
            "分析方法": selected_row["分析方法"],
            "理论基础": selected_row["理论基础"],
            "自变量": selected_row["自变量"],
            "中介/调节变量": selected_row["中介/调节变量"],
            "因变量/结果变量": selected_row["因变量/结果变量"],
            "控制变量": selected_row["控制变量"],
            "未来研究方向": selected_row["未来研究方向"],
            "未来研究编码": selected_row["未来研究编码"],
            "文件路径": selected_row["文件路径"],
            "批次": selected_row["批次"],
        }
        st.table(pd.DataFrame({"字段": list(detail_map.keys()), "内容": list(detail_map.values())}))
        st.markdown("#### 当前行 Prompt")
        st.code(str(selected_row["变量筛选prompt"]))

        st.markdown("### 第一步说明")
        st.info(
            "这一步只负责提取论文信息、变量、主要概念、主要观点和默认 prompt。"
            "你后面把第二步、第三步的逻辑告诉我后，我会继续把论文编码工作台完整接上。"
        )

    with step2_tab:
        st.markdown("### 第二步预留区")
        st.info("你后续告诉我第二步流程后，我会把它接在第一步主表结果后面。")

    with step3_tab:
        st.markdown("### 第三步预留区")
        st.info("你后续告诉我第三步流程后，我会把它继续接成完整自动化。")


def meta_analysis_panel() -> None:
    st.subheader("元分析工作台")
    st.caption("用于导入文献并自动分批，先搭建元分析资料清单、提取模板和后续运行底座。")

    with st.form("meta-analysis-form"):
        project_name = st.text_input("项目名称", value="元分析工作台")
        source_inputs = source_import_block("meta", allowed_hint="pdf/docx/txt/md/csv/xlsx")
        max_files = st.slider("每批最多文件数", min_value=5, max_value=100, value=25, step=1, key="meta-max-files")
        max_pages = st.slider("每批最多估算页数", min_value=100, max_value=1500, value=600, step=50, key="meta-max-pages")
        max_size = st.slider("每批最大体积（MB）", min_value=20, max_value=600, value=160, step=10, key="meta-max-size")
        submitted = st.form_submit_button("生成元分析工作区", type="primary")

    if not submitted:
        return

    run_dir = RUNS_ROOT / f"meta_analysis_{timestamp_id()}"
    prepared = prepare_batches(
        run_dir=run_dir,
        allowed_suffixes=META_ANALYSIS_SUFFIXES,
        selected_desktop_names=source_inputs["selected_desktop"],
        custom_paths_text=source_inputs["custom_paths"],
        uploaded_files=source_inputs["uploaded_files"],
        max_files_per_batch=max_files,
        max_pages_per_batch=max_pages,
        max_size_mb_per_batch=max_size,
    )
    if not prepared["files"]:
        st.warning("没有扫描到可用于元分析的文件。")
        return

    render_batch_preview(prepared, title="元分析批次预览")
    template_path = Path(build_meta_analysis_template(run_dir, prepared["files"], prepared["batches"]))
    st.success(f"{project_name} 的元分析工作区已生成。")
    render_download(template_path, "下载元分析提取模板 CSV", "text/csv")
    if template_path.exists():
        st.dataframe(pd.read_csv(template_path).head(50), use_container_width=True)
    st.info("目前这一步先完成资料归档、自动分批和提取模板准备。你后面告诉我元分析具体流程后，我会把效应值抽取、编码规则和汇总逻辑接上。")


def interview_coding_panel() -> None:
    st.subheader("资料 / 访谈编码工作台")
    st.caption("用于导入访谈、会议纪要、田野资料、政策材料等文本，自动切割成编码分段并分批处理。")

    with st.form("interview-coding-form"):
        project_name = st.text_input("项目名称", value="资料访谈编码工作台")
        source_inputs = source_import_block("interview", allowed_hint="docx/txt/md/pdf")
        max_files = st.slider("每批最多文件数", min_value=5, max_value=100, value=30, step=1, key="interview-max-files")
        max_pages = st.slider("每批最多估算页数", min_value=100, max_value=2000, value=800, step=50, key="interview-max-pages")
        max_size = st.slider("每批最大体积（MB）", min_value=20, max_value=600, value=150, step=10, key="interview-max-size")
        chunk_chars = st.slider("单段切割字符数", min_value=500, max_value=5000, value=2200, step=100)
        submitted = st.form_submit_button("生成访谈编码工作区", type="primary")

    if not submitted:
        return

    run_dir = RUNS_ROOT / f"interview_coding_{timestamp_id()}"
    prepared = prepare_batches(
        run_dir=run_dir,
        allowed_suffixes=INTERVIEW_SUFFIXES,
        selected_desktop_names=source_inputs["selected_desktop"],
        custom_paths_text=source_inputs["custom_paths"],
        uploaded_files=source_inputs["uploaded_files"],
        max_files_per_batch=max_files,
        max_pages_per_batch=max_pages,
        max_size_mb_per_batch=max_size,
    )
    if not prepared["files"]:
        st.warning("没有扫描到可用于访谈编码的文件。")
        return

    render_batch_preview(prepared, title="资料 / 访谈批次预览")
    segment_paths = build_interview_segments(run_dir, prepared["files"], chunk_chars=chunk_chars)
    segment_csv = Path(segment_paths["segment_csv"])
    st.success(f"{project_name} 的访谈编码工作区已生成。")
    render_download(segment_csv, "下载访谈分段 CSV", "text/csv")
    render_download(Path(segment_paths["segment_jsonl"]), "下载访谈分段 JSONL", "application/json")
    if segment_csv.exists():
        st.dataframe(pd.read_csv(segment_csv).head(80), use_container_width=True)
    st.info("目前这一步先完成资料导入、自动切割和编码包生成。你后面把访谈编码逻辑告诉我后，我会继续接上开放编码、主轴编码和主题归纳。")


def deep_research_panel() -> None:
    st.subheader("行业深度研究报告")
    st.caption("这是你之前那条多智能体工作流，保留在这里作为扩展模块。")

    with st.form("deep-research-form"):
        task = st.text_area("研究任务", value="比较腾讯、苹果和特斯拉在平台生态与资本市场表现上的差异", height=80)
        symbols = st.text_input("标的代码", value="0700.HK,AAPL,TSLA")
        metrics = st.text_input("指标", value="收盘价,区间涨跌幅,成交活跃度,市值,PE,ROE,净利率,营收,净利润")
        keywords = st.text_input("关键词", value="平台生态,AI,舆情,政策,社区讨论")
        market_scope = st.text_input("市场范围", value="CN,HK,US")
        output_name = st.text_input("输出名称", value="deep_research_demo")

        st.markdown("#### 可选：复用论文编码结果")
        use_latest_paper_coding = st.checkbox("自动复用最近一次论文编码合并结果", value=True)
        literature_csv_upload = st.file_uploader("或上传已有 literature_table.csv", type=["csv"], key="deep-upload-csv")
        local_pdf_files = st.file_uploader("上传额外 PDF", type=["pdf"], accept_multiple_files=True, key="deep-upload-pdf")
        include_policy_digest = st.checkbox(
            "纳入最新政策库与新闻快照",
            value=policy_snapshot_available(),
            help="会把最新政策 / 新闻快照作为真实输入源接入行业报告工作流。",
        )

        st.markdown("#### 可选：模型增强")
        enable_llm = st.checkbox("启用 OpenAI 兼容接口增强报告生成", value=False)
        api_url = st.text_input("深度研究 API 地址", value="", placeholder="https://your-api.example.com/v1")
        model_name = st.text_input("深度研究模型名称", value="", placeholder="gpt-4o-mini")
        api_key = st.text_input("深度研究 API Key", value="", type="password")
        submitted = st.form_submit_button("生成深度研究报告", type="primary")

    if not submitted:
        return

    run_dir = RUNS_ROOT / f"deep_research_{timestamp_id()}"
    upload_dir = run_dir / "uploaded_pdfs"
    save_uploaded_files(local_pdf_files or [], upload_dir)

    literature_csv = ""
    latest_paper_coding_run = st.session_state.get("latest_paper_coding_run", "")
    if use_latest_paper_coding and latest_paper_coding_run:
        candidate = Path(latest_paper_coding_run) / "paper_coding_merged" / "merged_literature_table.csv"
        if candidate.exists():
            literature_csv = str(candidate)
    if literature_csv_upload is not None:
        literature_dir = run_dir / "uploaded_literature"
        literature_dir.mkdir(parents=True, exist_ok=True)
        uploaded_path = literature_dir / literature_csv_upload.name
        uploaded_path.write_bytes(literature_csv_upload.getbuffer())
        literature_csv = str(uploaded_path)

    config = base_deep_research_config(run_dir, literature_csv=literature_csv)
    config["local_pdf_paths"] = [str(upload_dir)] if any(upload_dir.glob("*.pdf")) else []
    if include_policy_digest and policy_snapshot_available():
        latest_dir = POLICY_OUTPUT_ROOT / "latest"
        config["local_text_paths"] = list(dict.fromkeys(config.get("local_text_paths", []) + [str(latest_dir)]))
        structured_paths = list(config.get("structured_data_paths", []))
        for candidate in [latest_dir / "core_policies.csv", latest_dir / "all_policies.csv", latest_dir / "news_updates.csv"]:
            if candidate.exists():
                structured_paths.append(str(candidate))
        config["structured_data_paths"] = list(dict.fromkeys(structured_paths))
    if enable_llm and api_url.strip() and model_name.strip() and api_key.strip():
        config["llm"]["enabled"] = True
        config["llm"]["api_url"] = api_url.strip()
        config["llm"]["model"] = model_name.strip()

    config_path = run_dir / "deep_research_config.json"
    write_json(config_path, config)
    env = {"DEEP_RESEARCH_API_KEY": api_key.strip()} if enable_llm else {}
    args = [
        PYTHON_BIN,
        "deep_research_workflow.py",
        "--config",
        str(config_path),
        "--task",
        task.strip(),
        "--symbols",
        symbols.strip(),
        "--metrics",
        metrics.strip(),
        "--keywords",
        keywords.strip(),
        "--market-scope",
        market_scope.strip(),
        "--output-name",
        safe_name(output_name),
    ]

    with st.spinner("正在采集多源数据并生成报告..."):
        code, stdout, stderr = run_command(args, env)
    if code != 0:
        st.error("深度研究工作流运行失败。")
        if stderr.strip():
            st.code(stderr)
        return

    summary = extract_last_json_block(stdout)
    report_path = Path(summary.get("report_path", ""))
    payload_path = Path(summary.get("payload_path", ""))
    st.success("深度研究报告已生成。")
    if include_policy_digest and policy_snapshot_available():
        st.info("本次行业报告已经接入最新政策库与新闻快照。")
    col1, col2 = st.columns(2)
    col1.metric("采集证据数", int(summary.get("items_collected", 0)))
    col2.metric("图表数", len(summary.get("charts", [])))
    render_markdown_file(report_path, "研究报告预览")
    render_download(report_path, "下载研究报告 Markdown", "text/markdown")
    render_download(payload_path, "下载结构化 Payload", "application/json")

    for chart in summary.get("charts", []):
        chart_path = Path(chart)
        if chart_path.exists():
            st.image(str(chart_path), caption=chart_path.name, use_container_width=True)


def home_panel() -> None:
    st.title("研究资料自动化工作台")
    st.markdown(
        """
现在这个入口已经按你的想法拆成了三类主功能：

- `人工智能政策汇总`：汇总中国历年人工智能相关政策，区分核心政策和全部政策，并预留每日抓取页面
- `文献自动化编码`：新的三步式 UI，顶部箭头切换流程，先承接文献批量编码主流程
- `论文编码`：导入相关文献内容，按批次自动做论文编码
- `元分析`：导入研究文献，自动建元分析提取模板和批次工作区
- `资料 / 访谈编码`：导入访谈、纪要、政策材料等，自动切割成编码分段

它们的共同底层能力是：

- 直接调用桌面文件夹
- 支持手动路径和上传文件
- 文件过多时自动切割并分批
- 为每一批生成独立的工作区、清单和输入目录

你后面把每条流程的详细逻辑告诉我以后，我会继续把三条线分别接成完整自动化流水线。
        """
    )

    st.info("建议先从左侧的“文献自动化编码”开始。这个新入口已经先把三步式界面搭好了，后面再逐步接入功能。")

    st.markdown("### 当前功能结构")
    st.markdown(
        """
1. 人工智能政策汇总：新增政策总览页，先做核心政策 / 全部政策 / 每日抓取三层 UI
2. 文献自动化编码：新的三步式主入口，先做 UI，再逐步接论文编码逻辑
3. 论文编码工作台：现有批量论文编码功能，保留为旧版可运行入口
4. 元分析：先做资料整理、批次拆分和提取模板
5. 资料 / 访谈编码：先做文本切割、编码包生成和分段整理
        """
    )
    st.markdown("### 快速启动")
    st.code("python3 -m streamlit run streamlit_app.py", language="bash")
    st.markdown("### 运行目录")
    st.code(str(RUNS_ROOT))


def main() -> None:
    ensure_runtime_dirs()
    st.set_page_config(page_title="研究资料自动化工作台", page_icon="📚", layout="wide")
    st.sidebar.title("功能导航")
    page_options = [
        "首页",
        "人工智能政策汇总",
        "文献自动化编码",
        "论文编码工作台",
        "元分析工作台",
        "资料 / 访谈编码工作台",
        "行业深度研究报告",
    ]
    requested_page = st.query_params.get("page", "首页")
    if requested_page not in page_options:
        requested_page = "首页"
    page = st.sidebar.radio(
        "选择工作台",
        options=page_options,
        index=page_options.index(requested_page),
    )
    st.query_params["page"] = page
    if page == "首页":
        home_panel()
    elif page == "人工智能政策汇总":
        policy_digest_panel()
    elif page == "文献自动化编码":
        literature_auto_coding_panel()
    elif page == "论文编码工作台":
        paper_coding_panel()
    elif page == "元分析工作台":
        meta_analysis_panel()
    elif page == "资料 / 访谈编码工作台":
        interview_coding_panel()
    else:
        deep_research_panel()


if __name__ == "__main__":
    main()
