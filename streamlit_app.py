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


APP_ROOT = Path(__file__).resolve().parent
RUNTIME_ROOT = APP_ROOT / "webui_runtime"
UPLOAD_ROOT = RUNTIME_ROOT / "uploads"
RUNS_ROOT = RUNTIME_ROOT / "runs"
PYTHON_BIN = sys.executable


def ensure_runtime_dirs() -> None:
    for path in [RUNTIME_ROOT, UPLOAD_ROOT, RUNS_ROOT]:
        path.mkdir(parents=True, exist_ok=True)


def timestamp_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def safe_name(text: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text.strip())
    return cleaned.strip("_") or "run"


def save_uploaded_files(files: list[Any], target_dir: Path) -> list[str]:
    saved: list[str] = []
    target_dir.mkdir(parents=True, exist_ok=True)
    for uploaded in files:
        destination = target_dir / uploaded.name
        destination.write_bytes(uploaded.getbuffer())
        saved.append(str(destination))
    return saved


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


def base_grounded_config(run_dir: Path) -> dict[str, Any]:
    return {
        "project_name": "网页端扎根文献监测",
        "queries": [],
        "sources": ["local", "openalex", "arxiv", "semantic_scholar"],
        "days_back": 30,
        "max_results_per_query": 15,
        "download_pdfs": True,
        "max_pdf_pages_for_coding": 30,
        "sleep_seconds": 0.4,
        "outdir": str(run_dir / "grounded_output"),
        "baseline_paths": [],
        "covered_topics": [],
        "local_library_paths": [],
        "max_local_papers": 20,
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
    grounded_dir = run_dir / "grounded_output"
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
    if path.exists():
        st.markdown(f"### {title}")
        st.markdown(path.read_text(encoding="utf-8", errors="ignore"))


def grounded_monitor_panel() -> None:
    st.subheader("扎根文献监测")
    st.caption("适合做研究假设、理论命题、变量角色和未来研究方向的持续编码。")

    with st.form("grounded-monitor-form"):
        project_name = st.text_input("项目名称", value="网页端扎根文献监测")
        queries_text = st.text_area(
            "研究主题词",
            value="创业 即兴行为 扎根理论\n创业 资源视角 扎根理论",
            height=100,
            help="每行一条检索词。",
        )
        covered_text = st.text_area(
            "你已经覆盖的主题",
            value="前因\n结果\n边界条件\n创业资源\n创业网络",
            height=100,
        )
        sources = st.multiselect(
            "检索来源",
            options=["local", "openalex", "arxiv", "semantic_scholar"],
            default=["openalex", "arxiv", "semantic_scholar"],
        )
        baseline_files = st.file_uploader(
            "上传你的论文或基准文献",
            type=["pdf", "txt", "md", "docx"],
            accept_multiple_files=True,
        )
        local_pdf_files = st.file_uploader(
            "上传待扫描的本地文献 PDF",
            type=["pdf"],
            accept_multiple_files=True,
        )
        days_back = st.slider("回溯天数", min_value=7, max_value=365, value=45, step=1)
        max_results = st.slider("每条检索词最大结果数", min_value=5, max_value=50, value=15, step=1)
        enable_agent = st.checkbox("启用内置研究 agent 轨迹", value=True)

        st.markdown("#### 可选：模型增强")
        enable_llm = st.checkbox("启用 OpenAI 兼容接口增强编码/问答/报告", value=False)
        api_url = st.text_input("API 地址", value="", placeholder="https://your-api.example.com/v1")
        model_name = st.text_input("模型名称", value="", placeholder="gpt-4o-mini 或其他兼容模型")
        api_key = st.text_input("API Key", value="", type="password")

        submitted = st.form_submit_button("开始监测", type="primary")

    if not submitted:
        return

    run_dir = RUNS_ROOT / f"grounded_{timestamp_id()}"
    upload_dir = run_dir / "uploaded_pdfs"
    baseline_dir = run_dir / "baseline_files"
    baseline_paths = save_uploaded_files(baseline_files or [], baseline_dir)
    pdf_paths = save_uploaded_files(local_pdf_files or [], upload_dir)

    config = base_grounded_config(run_dir)
    config["project_name"] = project_name.strip() or config["project_name"]
    config["queries"] = [line.strip() for line in queries_text.splitlines() if line.strip()]
    config["covered_topics"] = [line.strip() for line in covered_text.splitlines() if line.strip()]
    config["baseline_paths"] = baseline_paths
    config["local_library_paths"] = [str(upload_dir)] if pdf_paths else []
    config["sources"] = sources or ["openalex"]
    config["days_back"] = days_back
    config["max_results_per_query"] = max_results
    config["agent"]["enabled"] = enable_agent
    if enable_llm and api_url.strip() and model_name.strip() and api_key.strip():
        config["assistant"]["enabled"] = True
        config["assistant"]["api_url"] = api_url.strip()
        config["assistant"]["model"] = model_name.strip()

    config_path = run_dir / "grounded_config.json"
    write_json(config_path, config)
    env = {"GROUNDED_AGENT_API_KEY": api_key.strip()} if enable_llm else {}

    with st.spinner("正在检索文献并生成编码，请稍候..."):
        code, stdout, stderr = run_command(
            [PYTHON_BIN, "grounded_daily_monitor.py", "--config", str(config_path)],
            env,
        )

    if code != 0:
        st.error("运行失败。")
        if stderr.strip():
            st.code(stderr)
        return

    summary = extract_last_json_block(stdout)
    st.success("扎根文献监测已完成。")
    st.session_state["latest_grounded_run"] = str(run_dir)
    st.session_state["latest_grounded_config"] = str(config_path)
    st.session_state["latest_grounded_outdir"] = summary.get("results_csv", "")
    st.session_state["grounded_api_key"] = api_key.strip() if enable_llm else ""

    results_csv = Path(summary.get("results_csv", run_dir / "grounded_output" / "literature_table.csv"))
    results_xlsx = Path(summary.get("results_xlsx", run_dir / "grounded_output" / "literature_table.xlsx"))
    daily_report = Path(summary.get("daily_report", run_dir / "grounded_output" / "daily_report.md"))

    col1, col2, col3 = st.columns(3)
    col1.metric("新增文献", int(summary.get("new_rows", 0)))
    col2.metric("总文献数", int(summary.get("total_rows", 0)))
    col3.metric("输出目录", 1 if results_csv.exists() else 0, help=str(run_dir / "grounded_output"))

    if results_csv.exists():
        st.markdown("### 文献总表预览")
        st.dataframe(pd.read_csv(results_csv).head(30), use_container_width=True)
    render_download(results_csv, "下载 CSV 总表", "text/csv")
    render_download(results_xlsx, "下载 Excel 总表", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    render_markdown_file(daily_report, "日报预览")

    with st.expander("运行日志"):
        if stdout.strip():
            st.code(stdout)
        if stderr.strip():
            st.code(stderr)


def grounded_assistant_panel() -> None:
    st.subheader("文献问答与行业报告")
    latest_config = st.session_state.get("latest_grounded_config", "")
    latest_run = st.session_state.get("latest_grounded_run", "")
    if not latest_config or not Path(latest_config).exists():
        st.info("请先在“扎根文献监测”里跑出一版文献库，再来做问答或行业报告。")
        return

    st.caption(f"当前复用的监测配置：`{latest_config}`")
    question = st.text_area("输入你的研究问题", value="当前创业即兴行为研究中最常见的前因和边界条件是什么？", height=80)
    report_topic = st.text_area("输入行业报告主题", value="生成式AI在创业研究与创新管理中的应用", height=80)

    col1, col2 = st.columns(2)
    ask_clicked = col1.button("生成文献问答", type="primary")
    report_clicked = col2.button("生成行业报告", type="primary")

    if ask_clicked and question.strip():
        with st.spinner("正在生成问答..."):
            code, stdout, stderr = run_command(
                [PYTHON_BIN, "grounded_daily_monitor.py", "--config", latest_config, "--skip-monitor", "--ask", question.strip()],
                {"GROUNDED_AGENT_API_KEY": st.session_state.get("grounded_api_key", "")},
            )
        if code != 0:
            st.error("文献问答生成失败。")
            if stderr.strip():
                st.code(stderr)
            return
        summary = extract_last_json_block(stdout)
        qa_path = Path(summary.get("qa_answer", ""))
        st.success("问答已生成。")
        render_markdown_file(qa_path, "问答结果")
        render_download(qa_path, "下载问答 Markdown", "text/markdown")

    if report_clicked and report_topic.strip():
        with st.spinner("正在生成行业报告..."):
            code, stdout, stderr = run_command(
                [PYTHON_BIN, "grounded_daily_monitor.py", "--config", latest_config, "--skip-monitor", "--generate-report", report_topic.strip()],
                {"GROUNDED_AGENT_API_KEY": st.session_state.get("grounded_api_key", "")},
            )
        if code != 0:
            st.error("行业报告生成失败。")
            if stderr.strip():
                st.code(stderr)
            return
        summary = extract_last_json_block(stdout)
        report_path = Path(summary.get("industry_report", ""))
        st.success("行业报告已生成。")
        render_markdown_file(report_path, "行业报告预览")
        render_download(report_path, "下载行业报告 Markdown", "text/markdown")

    if latest_run:
        outdir = Path(latest_run) / "grounded_output"
        st.caption(f"当前结果目录：`{outdir}`")


def deep_research_panel() -> None:
    st.subheader("多智能体深度研究报告")
    st.caption("适合做公司、行业、跨市场对比与三层舆情分析。")

    with st.form("deep-research-form"):
        task = st.text_area("研究任务", value="比较腾讯、苹果和特斯拉在平台生态与资本市场表现上的差异", height=80)
        symbols = st.text_input("标的代码", value="0700.HK,AAPL,TSLA")
        metrics = st.text_input("指标", value="收盘价,区间涨跌幅,成交活跃度,市值,PE,ROE,净利率,营收,净利润")
        keywords = st.text_input("关键词", value="平台生态,AI,舆情,政策,社区讨论")
        market_scope = st.text_input("市场范围", value="CN,HK,US")
        output_name = st.text_input("输出名称", value="deep_research_demo")

        st.markdown("#### 可选：复用扎根文献库")
        use_latest_grounded = st.checkbox("自动复用最近一次扎根监测的 literature_table.csv", value=True)
        literature_csv_upload = st.file_uploader("或上传已有 literature_table.csv", type=["csv"])
        local_pdf_files = st.file_uploader("上传额外 PDF", type=["pdf"], accept_multiple_files=True)

        st.markdown("#### 可选：模型增强")
        enable_llm = st.checkbox("启用 OpenAI 兼容接口增强报告生成", value=False)
        api_url = st.text_input("深度研究 API 地址", value="", placeholder="https://your-api.example.com/v1")
        model_name = st.text_input("深度研究模型名称", value="", placeholder="gpt-4o-mini 或其他兼容模型")
        api_key = st.text_input("深度研究 API Key", value="", type="password")

        submitted = st.form_submit_button("生成深度研究报告", type="primary")

    if not submitted:
        return

    run_dir = RUNS_ROOT / f"deep_research_{timestamp_id()}"
    upload_dir = run_dir / "uploaded_pdfs"
    save_uploaded_files(local_pdf_files or [], upload_dir)

    literature_csv = ""
    latest_grounded_run = st.session_state.get("latest_grounded_run", "")
    if use_latest_grounded and latest_grounded_run:
        candidate = Path(latest_grounded_run) / "grounded_output" / "literature_table.csv"
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
    st.session_state["latest_deep_report"] = str(report_path)

    col1, col2 = st.columns(2)
    col1.metric("采集证据数", int(summary.get("items_collected", 0)))
    col2.metric("图表数", len(summary.get("charts", [])))

    render_markdown_file(report_path, "研究报告预览")
    render_download(report_path, "下载研究报告 Markdown", "text/markdown")
    render_download(payload_path, "下载结构化 Payload", "application/json")

    charts = summary.get("charts", [])
    if charts:
        st.markdown("### 图表")
        for chart in charts:
            chart_path = Path(chart)
            if chart_path.exists():
                st.image(str(chart_path), caption=chart_path.name, use_container_width=True)

    with st.expander("运行日志"):
        if stdout.strip():
            st.code(stdout)
        if stderr.strip():
            st.code(stderr)


def home_panel() -> None:
    st.title("扎根研究工作台")
    st.markdown(
        """
这个网页入口把仓库里的两条主线包装成了一个可直接操作的中文界面：

- 扎根文献监测：自动搜文献、做编码、产出表格、提醒研究缺口
- 文献问答与行业报告：直接复用已有文献库回答问题或写主题报告
- 多智能体深度研究：汇总财务、行情、新闻、政策、社区，生成多章节研究报告
        """
    )
    st.info("建议第一次使用时，先在左侧切到“扎根文献监测”，跑出一版 literature_table.csv。")

    st.markdown("### 快速开始")
    st.code(
        "pip install -r requirements.txt\npython3 -m streamlit run streamlit_app.py",
        language="bash",
    )

    st.markdown("### 当前运行目录")
    st.code(str(RUNTIME_ROOT))


def main() -> None:
    ensure_runtime_dirs()
    st.set_page_config(
        page_title="扎根研究工作台",
        page_icon="📚",
        layout="wide",
    )

    st.sidebar.title("功能导航")
    page = st.sidebar.radio(
        "选择页面",
        options=["首页", "扎根文献监测", "文献问答与行业报告", "多智能体深度研究"],
    )

    if page == "首页":
        home_panel()
    elif page == "扎根文献监测":
        grounded_monitor_panel()
    elif page == "文献问答与行业报告":
        grounded_assistant_panel()
    else:
        deep_research_panel()


if __name__ == "__main__":
    main()
