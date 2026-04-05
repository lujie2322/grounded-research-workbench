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


def reorder_files_by_upload_order(files: list[Any], uploaded_files: list[Any]) -> list[Any]:
    if not uploaded_files:
        return files
    upload_order = {uploaded.name: index for index, uploaded in enumerate(uploaded_files)}
    ordered: list[tuple[int, int, Any]] = []
    for index, item in enumerate(files):
        priority = upload_order.get(Path(item.path).name, len(upload_order) + index)
        ordered.append((priority, index, item))
    ordered.sort(key=lambda row: (row[0], row[1]))
    return [item for _, _, item in ordered]


def build_attachment_seed_table(uploaded_files: list[Any], rows: int = 18) -> pd.DataFrame:
    prompt = "我将按照学术文献解构框架，系统分析这篇关于企业人工智能采纳的实证研究论文。"
    attachment_names = [uploaded.name for uploaded in uploaded_files]
    total_rows = max(rows, len(attachment_names) + 8)
    data = []
    for index in range(1, total_rows + 1):
        data.append(
            {
                "序号": index,
                "附件": attachment_names[index - 1] if index - 1 < len(attachment_names) else "",
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
                "附件": "",
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
    st.session_state["auto_coding_stage1_run"] = str(run_dir)
    st.session_state["auto_coding_stage1_inventory"] = prepared["inventory_paths"]
    st.session_state["auto_coding_stage1_batch_count"] = len(prepared["batches"])
    st.session_state["auto_coding_stage1_file_count"] = len(prepared["files"])
    st.session_state["auto_coding_stage1_rows"] = stage1_df.to_dict(orient="records")
    st.session_state["auto_coding_stage1_csv"] = output_paths["csv"]
    st.session_state["auto_coding_stage1_xlsx"] = output_paths["xlsx"]


def render_auto_coding_stage1_results() -> None:
    rows = st.session_state.get("auto_coding_stage1_rows", [])
    if not rows:
        return

    stage1_df = pd.DataFrame(rows)
    st.markdown("### 第一步主表")
    st.caption("这里已经接入真实提取结果。你可以直接在表格里修改主要概念、主要观点和每篇文献的变量筛选 prompt。")
    column_order = ["序号", "附件预览", "附件", "主要概念", "主要观点", "变量筛选prompt"]
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
            "主要概念": st.column_config.TextColumn("主要概念", width="medium"),
            "主要观点": st.column_config.TextColumn("主要观点", width="large"),
            "变量筛选prompt": st.column_config.TextColumn("变量筛选prompt", width="large"),
        },
        key="auto-coding-stage1-editor-live",
    )
    st.session_state["auto_coding_stage1_rows"] = pd.DataFrame(edited_df).to_dict(orient="records")
    run_dir_raw = st.session_state.get("auto_coding_stage1_run", "")
    if run_dir_raw:
        edited_output = save_stage1_outputs(Path(run_dir_raw), pd.DataFrame(edited_df))
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

    st.markdown("### 论文详情查看")
    detail_df = pd.DataFrame(edited_df)
    options = [f"{int(row['序号'])}. {row['附件']} | {row['标题']}" for _, row in detail_df.iterrows()]
    if options:
        selected_option = st.selectbox("选择一篇文献查看详细提取结果", options=options, key="auto-coding-detail-select")
        selected_index = options.index(selected_option)
        selected_row = detail_df.iloc[selected_index]
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
                output_paths = save_stage1_outputs(run_dir, stage1_df)
                store_auto_coding_stage1_results(
                    run_dir=run_dir,
                    prepared=prepared,
                    stage1_df=stage1_df,
                    output_paths=output_paths,
                )
                st.success(f"{project_name} 的第一步提取表已生成。")

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
            st.data_editor(
                build_attachment_seed_table(uploaded_seed) if uploaded_seed else build_placeholder_stage1_table(),
                hide_index=True,
                use_container_width=True,
                height=520,
                num_rows="dynamic",
                column_config={
                    "序号": st.column_config.NumberColumn("序号", disabled=True, width="small"),
                    "附件": st.column_config.TextColumn("附件", width="small"),
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
    st.session_state["grounded_api_key"] = api_key.strip() if enable_llm else ""
    render_batch_preview(prepared, title="第一步：批次拆分预览")

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
1. 文献自动化编码：新的三步式主入口，先做 UI，再逐步接论文编码逻辑
2. 论文编码工作台：现有批量论文编码功能，保留为旧版可运行入口
3. 元分析：先做资料整理、批次拆分和提取模板
4. 资料 / 访谈编码：先做文本切割、编码包生成和分段整理
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
