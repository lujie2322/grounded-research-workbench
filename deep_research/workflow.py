from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import Any
from io import StringIO

import matplotlib.pyplot as plt
import pandas as pd

from grounded_daily_monitor import load_existing_rows

from .connectors import (
    CommunityConnector,
    FinanceConnector,
    LocalPdfConnector,
    LocalStructuredConnector,
    LocalTextConnector,
    NewsConnector,
    PolicyConnector,
    normalize_yahoo_symbol,
    plain_symbol,
)
from .llm import OpenAICompatibleLLM
from .memory import WorkflowMemory
from .models import AnalysisArtifact, CollectedItem, ReportArtifact, ResearchTask, SearchPlan, WorkflowConfig
from .utils import load_csv_rows, load_json, log_trace, normalize_text, save_json, score_text, tokenize


DEFAULT_WORKFLOW = {
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
}


class SearcherAgent:
    def __init__(self, trace_path: Path) -> None:
        self.trace_path = trace_path

    def run(self, task: ResearchTask) -> SearchPlan:
        keywords = task.keywords or tokenize(task.task)
        focus = [
            "执行摘要",
            "行业现状",
            "财务与量化信号",
            "新闻舆情",
            "政策动向",
            "社区讨论",
            "风险与机会",
            "未来情景",
        ]
        plan = SearchPlan(
            task=task.task,
            intent=task.mode,
            keywords=keywords[:18],
            structured_targets=["market_data", "financial_statements", "macro_indicators"],
            unstructured_targets=["literature", "news", "policy", "community_sentiment"],
            required_agents=["orchestrator", "searcher", "collector", "analyst", "aggregator"],
            report_focus=focus,
        )
        log_trace(self.trace_path, "Searcher", "plan", "ok", keywords=plan.keywords, focus=plan.report_focus)
        return plan


class CollectorAgent:
    def __init__(self, config: WorkflowConfig, trace_path: Path) -> None:
        self.config = config
        self.trace_path = trace_path
        self.finance = FinanceConnector(config.connectors.get("finance"))
        self.news = NewsConnector(config.connectors.get("news"))
        self.policy = PolicyConnector(config.connectors.get("policy"))
        self.community = CommunityConnector(config.connectors.get("community"))
        self.local_structured = LocalStructuredConnector()
        self.local_text = LocalTextConnector()
        self.local_pdf = LocalPdfConnector()

    def collect(self, plan: SearchPlan, task: ResearchTask) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        items.extend(self._collect_from_literature(plan))
        items.extend(
            self.local_structured.collect(
                self.config.structured_data_paths,
                plan.keywords,
            )[: int(self.config.workflow.get("max_structured_items", DEFAULT_WORKFLOW["max_structured_items"]))]
        )
        items.extend(
            self.local_text.collect(
                self.config.local_text_paths,
                plan.keywords,
            )[: int(self.config.workflow.get("max_text_items", DEFAULT_WORKFLOW["max_text_items"]))]
        )
        items.extend(
            self.local_pdf.collect(
                self.config.local_pdf_paths,
                plan.keywords,
            )[: int(self.config.workflow.get("max_text_items", DEFAULT_WORKFLOW["max_text_items"]))]
        )
        items.extend(self.finance.collect(task.symbols, task.metrics, plan.keywords))
        news_queries = [task.task] + extract_focus_terms(task.task)[:3] + plan.keywords[:1]
        items.extend(self.news.collect(news_queries)[: int(self.config.workflow.get("max_news_items", DEFAULT_WORKFLOW["max_news_items"]))])
        items.extend(self.policy.collect(plan.keywords[:4])[: int(self.config.workflow.get("max_policy_items", DEFAULT_WORKFLOW["max_policy_items"]))])
        items.extend(
            self.community.collect(task.symbols, plan.keywords)[
                : int(self.config.workflow.get("max_community_items", DEFAULT_WORKFLOW["max_community_items"]))
            ]
        )
        deduped: list[CollectedItem] = []
        seen_titles: set[str] = set()
        for item in sorted(items, key=lambda item: item.score, reverse=True):
            normalized_title = normalize_text(item.title)
            if normalized_title in seen_titles:
                continue
            seen_titles.add(normalized_title)
            deduped.append(item)
        items = deduped
        log_trace(self.trace_path, "Collector", "collect", "ok", items=len(items))
        return items

    def _collect_from_literature(self, plan: SearchPlan) -> list[CollectedItem]:
        if not self.config.literature_csv:
            return []
        rows = load_existing_rows(Path(self.config.literature_csv))
        items: list[CollectedItem] = []
        for row in rows:
            text = " ".join(
                [
                    row.title,
                    row.title_zh,
                    row.abstract,
                    row.abstract_zh,
                    row.axial_summary,
                    row.selective_summary,
                    row.antecedents,
                    row.outcomes,
                    row.mechanisms,
                    row.boundaries,
                    row.future_directions,
                    row.evidence_sentences,
                    row.recommendation,
                ]
            )
            score = score_text(text, plan.keywords)
            if score <= 0:
                continue
            items.append(
                CollectedItem(
                    item_id=row.external_id or row.doi or row.title,
                    source_type="literature",
                    source_name=row.source_name,
                    title=row.title,
                    summary=row.selective_summary or row.axial_summary or row.abstract[:300],
                    content=text[:8000],
                    metadata={
                        "year": row.year,
                        "journal": row.journal,
                        "cited_by_count": row.cited_by_count,
                        "novel_themes": row.novel_themes,
                        "local_pdf": row.local_pdf,
                    },
                    score=score + (0.2 if row.peer_reviewed == "是" else 0.0) + min(row.cited_by_count / 100.0, 1.0),
                )
            )
        limit = int(self.config.workflow.get("max_literature_items", DEFAULT_WORKFLOW["max_literature_items"]))
        return sorted(items, key=lambda item: item.score, reverse=True)[:limit]


class AnalystAgent:
    def __init__(self, config: WorkflowConfig, trace_path: Path, outdir: Path) -> None:
        self.config = config
        self.trace_path = trace_path
        self.outdir = outdir

    def analyze(self, task: ResearchTask, plan: SearchPlan, items: list[CollectedItem]) -> AnalysisArtifact:
        qualitative = [item for item in items if item.source_type in {"literature", "unstructured"}]
        quantitative = [item for item in items if item.source_type == "structured"]
        news_items = self._rank_items([item for item in items if item.source_type == "news"], task, plan, "news")
        policy_items = self._rank_items([item for item in items if item.source_type == "policy"], task, plan, "policy")
        community_items = self._rank_items([item for item in items if item.source_type == "community"], task, plan, "community")
        macro_items = [item for item in items if item.source_type == "macro"]
        quantitative = self._rank_items(quantitative, task, plan, "structured")
        qualitative = self._rank_items(qualitative, task, plan, "qualitative")

        artifact = AnalysisArtifact()
        artifact.qualitative_findings = self._qualitative_findings(qualitative)
        artifact.quantitative_findings = self._quantitative_findings(quantitative, macro_items)
        artifact.financial_comparison = self._financial_comparison(quantitative)
        artifact.sentiment_findings = self._sentiment_findings(news_items)
        artifact.policy_findings = self._policy_findings(policy_items)
        artifact.community_findings = self._community_findings(community_items)
        artifact.scorecard_rows = self._scorecard_rows(quantitative)
        artifact.sentiment_dashboard = self._sentiment_dashboard(news_items, policy_items, community_items)
        artifact.priority_evidence = self._priority_evidence(quantitative, news_items, policy_items, community_items, qualitative)
        artifact.risks = self._extract_risks(qualitative + news_items + policy_items + community_items, quantitative + macro_items)
        artifact.opportunities = self._extract_opportunities(
            qualitative + news_items + policy_items + community_items,
            quantitative + macro_items,
        )
        artifact.scenarios = self._build_scenarios(task, artifact)
        artifact.consistency_checks = self._consistency_checks(artifact)
        artifact.evidence_map = self._build_evidence_map(
            qualitative,
            quantitative,
            news_items,
            policy_items,
            community_items,
            macro_items,
        )
        artifact.chart_paths = self._generate_charts(
            qualitative,
            quantitative,
            news_items,
            policy_items,
            community_items,
            macro_items,
        )
        artifact.notes = [
            "本轮已优先尝试 Baostock 获取 A 股行情与财务数据，并使用 Yahoo Finance 补充港股、美股行情与财务快照。",
            "政策检索来自国务院政策文件库官方接口，新闻检索来自 Google 新闻 RSS 与 Akshare 个股新闻。",
            "社区讨论当前来自东方财富股吧与 Stocktwits，用于形成新闻、政策、社区三层情绪结构。",
            "量化结果默认在本地 Python 沙箱中执行；当前环境未检测到 Docker。",
        ]
        log_trace(
            self.trace_path,
            "Analyst",
            "analyze",
            "ok",
            qualitative=len(qualitative),
            quantitative=len(quantitative) + len(macro_items),
            news=len(news_items),
            policy=len(policy_items),
            community=len(community_items),
            charts=len(artifact.chart_paths),
        )
        return artifact

    def _qualitative_findings(self, items: list[CollectedItem]) -> list[str]:
        findings: list[str] = []
        for item in items[:8]:
            findings.append(f"{item.title} 指向：{item.summary[:220]}")
        if not findings:
            findings.append("当前没有足够的非结构化资料，舆情与政策判断需要补充新闻、政策和社区内容。")
        return findings

    def _quantitative_findings(self, items: list[CollectedItem], macro_items: list[CollectedItem]) -> list[str]:
        findings: list[str] = []
        for item in items[:6]:
            findings.append(f"{item.title}：{item.summary}")
        for item in macro_items[:3]:
            findings.append(f"{item.title}：{item.summary}")
        if not findings:
            findings.append("当前没有可用的结构化金融数据，财务比对与行情判断尚未展开。")
        return findings

    def _rank_items(
        self,
        items: list[CollectedItem],
        task: ResearchTask,
        plan: SearchPlan,
        layer: str,
    ) -> list[CollectedItem]:
        for item in items:
            item.metadata["relevance_score"] = self._item_relevance(item, task, plan, layer)
        return sorted(items, key=lambda item: (float(item.metadata.get("relevance_score", 0.0)), item.score), reverse=True)

    def _item_relevance(self, item: CollectedItem, task: ResearchTask, plan: SearchPlan, layer: str) -> float:
        title_text = normalize_text(item.title)
        body_text = normalize_text(" ".join([item.title, item.summary, item.content[:1600]]))
        keyword_hits = sum(1 for keyword in plan.keywords[:12] if keyword.lower() in body_text)
        title_hits = sum(1 for keyword in plan.keywords[:12] if keyword.lower() in title_text)
        focus_hits = sum(1 for token in self._focus_terms(task) if token.lower() in body_text)
        symbol_hits = sum(1 for token in self._symbol_tokens(task.symbols) if token and token.lower() in body_text)
        freshness = self._freshness_boost(item.metadata.get("published_at"))
        direct = 1.2 if symbol_hits else 0.0
        source_boost = {
            "news": {"Akshare 新闻": 0.9, "Google 新闻 RSS": 0.6},
            "policy": {"国务院政策库": 0.8},
            "community": {"东方财富股吧": 0.7, "Stocktwits": 0.5},
            "structured": {"Baostock": 0.9, "Yahoo Finance": 0.9, "Akshare": 0.5},
            "qualitative": {"本地 PDF": 0.4, "本地文本": 0.3},
        }.get(layer, {}).get(item.source_name, 0.0)
        if layer == "policy" and keyword_hits == 0 and title_hits == 0:
            source_boost -= 0.8
        if layer == "community" and symbol_hits == 0:
            source_boost -= 0.5
        if layer in {"news", "community"} and task.symbols and symbol_hits == 0 and focus_hits == 0:
            source_boost -= 1.4
        if layer == "community" and item.title.count("$") >= 4:
            source_boost -= 0.8
        return round(item.score + keyword_hits * 0.7 + title_hits * 0.9 + focus_hits * 1.2 + direct + freshness + source_boost, 3)

    def _symbol_tokens(self, symbols: list[str]) -> list[str]:
        tokens: set[str] = set()
        for raw_symbol in symbols:
            if not raw_symbol.strip():
                continue
            normalized = normalize_yahoo_symbol(raw_symbol)
            plain = plain_symbol(raw_symbol)
            for token in {
                raw_symbol.strip(),
                raw_symbol.strip().upper(),
                normalized,
                normalized.upper(),
                plain,
                plain.upper(),
                f"${plain.upper()}",
            }:
                if token:
                    tokens.add(token)
        return sorted(tokens)

    def _focus_terms(self, task: ResearchTask) -> list[str]:
        return extract_focus_terms(task.task)

    def _freshness_boost(self, published_at: Any) -> float:
        if not published_at:
            return 0.0
        try:
            timestamp = pd.to_datetime(str(published_at), errors="coerce", utc=True)
        except Exception:
            return 0.0
        if pd.isna(timestamp):
            return 0.0
        now = pd.Timestamp.now(tz="UTC")
        age_days = max((now - timestamp).days, 0)
        if age_days <= 30:
            return 0.8
        if age_days <= 90:
            return 0.5
        if age_days <= 365:
            return 0.2
        return 0.0

    def _financial_comparison(self, items: list[CollectedItem]) -> list[str]:
        comparison: list[str] = []
        kline_items = [item for item in items if item.metadata.get("dataset_type") == "kline"]
        if kline_items:
            ranked = sorted(
                [
                    (
                        item.metadata.get("symbol", item.title),
                        float(item.metadata.get("period_return_pct", 0.0)),
                        float(item.metadata.get("latest_close", 0.0)),
                        float(item.metadata.get("avg_turnover", item.metadata.get("avg_volume", 0.0))),
                        "平均换手率" if item.metadata.get("avg_turnover") is not None else "平均成交量",
                    )
                    for item in kline_items
                ],
                key=lambda entry: entry[1],
                reverse=True,
            )
            for symbol, ret, close, liquidity, liquidity_label in ranked[:6]:
                comparison.append(
                    f"{symbol}：区间涨跌幅 {ret:.2f}%，最新收盘 {close:.2f}，{liquidity_label} {liquidity:.2f}"
                )

        profit_items = [item for item in items if item.metadata.get("dataset_type") == "profit"]
        for item in profit_items[:6]:
            try:
                df = pd.read_csv(StringIO(item.content))
            except Exception:
                continue
            if df.empty:
                continue
            row = df.iloc[0].to_dict()
            symbol = item.metadata.get("symbol", item.title)
            roe = row.get("roeAvg", "")
            margin = row.get("npMargin", "")
            eps = row.get("epsTTM", "")
            comparison.append(f"{symbol}：ROE {roe}，净利率 {margin}，EPS(TTM) {eps}")

        dupont_items = [item for item in items if item.metadata.get("dataset_type") == "dupont"]
        for item in dupont_items[:4]:
            try:
                df = pd.read_csv(StringIO(item.content))
            except Exception:
                continue
            if df.empty:
                continue
            row = df.iloc[0].to_dict()
            symbol = item.metadata.get("symbol", item.title)
            roe = row.get("dupontROE", row.get("dupont_roe", row.get("dupontROE(%)", "")))
            asset_turn = row.get("assetTurn", row.get("asset_turn", ""))
            equity_mult = row.get("assetStoEquity", row.get("权益乘数", ""))
            comparison.append(f"{symbol}：杜邦口径 ROE {roe}，总资产周转率 {asset_turn}，权益乘数 {equity_mult}")

        snapshot_items = [item for item in items if item.metadata.get("dataset_type") == "equity_snapshot"]
        for item in snapshot_items[:6]:
            symbol = item.metadata.get("symbol", item.title)
            market_cap = format_large_number(item.metadata.get("market_cap"))
            current_price = item.metadata.get("current_price", "")
            trailing_pe = item.metadata.get("trailing_pe", "")
            roe = percent_or_value(item.metadata.get("return_on_equity"))
            margin = percent_or_value(item.metadata.get("profit_margin"))
            sector = item.metadata.get("sector", "")
            comparison.append(
                f"{symbol}：市值 {market_cap}，现价 {current_price}，PE(TTM) {trailing_pe}，ROE {roe}，净利率 {margin}，行业 {sector}"
            )

        income_items = [item for item in items if item.metadata.get("dataset_type") == "income_statement"]
        for item in income_items[:6]:
            symbol = item.metadata.get("symbol", item.title)
            period = item.metadata.get("latest_period", "")
            revenue = format_large_number(item.metadata.get("recent_revenue"))
            net_income = format_large_number(item.metadata.get("recent_net_income"))
            comparison.append(f"{symbol}：最新利润表期 {period}，营业收入 {revenue}，净利润 {net_income}")

        return comparison[:10] or ["当前没有足够的可比财务数据，无法形成公司间财务比对。"]

    def _sentiment_findings(self, items: list[CollectedItem]) -> list[str]:
        findings: list[str] = []
        for item in items[:8]:
            mood = self._classify_tone(item, "news")
            findings.append(f"{item.title}：舆情倾向 {mood}，来源 {item.source_name}")
        return findings[:8] or ["当前未抓到足够的新闻舆情结果。"]

    def _policy_findings(self, items: list[CollectedItem]) -> list[str]:
        findings: list[str] = []
        for item in items[:8]:
            tone = self._classify_tone(item, "policy")
            findings.append(
                f"{item.title}：政策倾向 {tone}，{item.summary[:120]}，类别 {item.metadata.get('category', 'unknown')}"
                .replace("unknown", "未标注")
            )
        return findings[:8] or ["当前未检索到足够的官方政策结果。"]

    def _community_findings(self, items: list[CollectedItem]) -> list[str]:
        findings: list[str] = []
        for item in items[:8]:
            mood = self._classify_tone(item, "community")
            findings.append(f"{item.title}：社区情绪 {mood}，来源 {item.source_name}")
        return findings[:8] or ["当前未抓到足够的社区讨论结果。"]

    def _scorecard_rows(self, items: list[CollectedItem]) -> list[dict[str, str]]:
        rows: dict[str, dict[str, str]] = {}
        for item in items:
            symbol = str(item.metadata.get("symbol", "")).strip()
            if not symbol:
                continue
            row = rows.setdefault(
                symbol,
                {
                    "标的": symbol,
                    "市场": str(item.metadata.get("market", "")).upper() or infer_market_label(symbol),
                    "价格": "暂无",
                    "区间涨跌幅": "暂无",
                    "活跃度": "暂无",
                    "市值": "暂无",
                    "PE": "暂无",
                    "ROE": "暂无",
                    "净利率": "暂无",
                    "营收": "暂无",
                    "净利润": "暂无",
                },
            )
            dataset_type = item.metadata.get("dataset_type")
            if dataset_type == "kline":
                row["价格"] = format_decimal(item.metadata.get("latest_close"))
                row["区间涨跌幅"] = percent_or_value(float(item.metadata.get("period_return_pct", 0.0)) / 100.0)
                liquidity = item.metadata.get("avg_turnover", item.metadata.get("avg_volume"))
                row["活跃度"] = format_large_number(liquidity)
            elif dataset_type == "equity_snapshot":
                row["市值"] = format_large_number(item.metadata.get("market_cap"))
                row["PE"] = format_decimal(item.metadata.get("trailing_pe"))
                row["ROE"] = percent_or_value(item.metadata.get("return_on_equity"))
                row["净利率"] = percent_or_value(item.metadata.get("profit_margin"))
            elif dataset_type == "income_statement":
                row["营收"] = format_large_number(item.metadata.get("recent_revenue"))
                row["净利润"] = format_large_number(item.metadata.get("recent_net_income"))
            elif dataset_type == "profit":
                try:
                    df = pd.read_csv(StringIO(item.content))
                except Exception:
                    df = pd.DataFrame()
                if not df.empty:
                    data = df.iloc[0].to_dict()
                    row["ROE"] = normalize_metric_display(data.get("roeAvg"), fallback=row["ROE"])
                    row["净利率"] = normalize_metric_display(data.get("npMargin"), fallback=row["净利率"])
        sorted_rows = sorted(
            rows.values(),
            key=lambda row: safe_float(row.get("区间涨跌幅", "0").replace("%", "")),
            reverse=True,
        )
        return sorted_rows[:8]

    def _sentiment_dashboard(
        self,
        news_items: list[CollectedItem],
        policy_items: list[CollectedItem],
        community_items: list[CollectedItem],
    ) -> list[dict[str, str]]:
        dashboard: list[dict[str, str]] = []
        for label, layer, items in [
            ("新闻舆情", "news", news_items),
            ("政策动向", "policy", policy_items),
            ("社区讨论", "community", community_items),
        ]:
            positive = 0
            neutral = 0
            cautious = 0
            sample = ""
            for index, item in enumerate(items[:6]):
                tone = self._classify_tone(item, layer)
                if tone == "偏积极":
                    positive += 1
                elif tone == "偏谨慎":
                    cautious += 1
                else:
                    neutral += 1
                if index == 0:
                    sample = item.title[:40]
            dominant = "偏中性"
            if positive > max(neutral, cautious):
                dominant = "偏积极"
            elif cautious > max(neutral, positive):
                dominant = "偏谨慎"
            dashboard.append(
                {
                    "层级": label,
                    "偏积极": str(positive),
                    "偏中性": str(neutral),
                    "偏谨慎": str(cautious),
                    "主导情绪": dominant,
                    "代表证据": sample or "暂无",
                }
            )
        return dashboard

    def _priority_evidence(
        self,
        quantitative: list[CollectedItem],
        news_items: list[CollectedItem],
        policy_items: list[CollectedItem],
        community_items: list[CollectedItem],
        qualitative: list[CollectedItem],
    ) -> dict[str, list[str]]:
        layers = {
            "财务数据": quantitative,
            "新闻舆情": news_items,
            "政策动向": policy_items,
            "社区讨论": community_items,
            "定性材料": qualitative,
        }
        result: dict[str, list[str]] = {}
        for layer, items in layers.items():
            result[layer] = [
                f"{item.title} | score={float(item.metadata.get('relevance_score', item.score)):.2f} | {item.source_name}"
                for item in items[:4]
            ]
        return result

    def _consistency_checks(self, artifact: AnalysisArtifact) -> list[str]:
        checks: list[str] = []
        market_signal = aggregate_market_signal(artifact.scorecard_rows)
        layer_tones = {row["层级"]: row["主导情绪"] for row in artifact.sentiment_dashboard}
        news_tone = layer_tones.get("新闻舆情", "偏中性")
        policy_tone = layer_tones.get("政策动向", "偏中性")
        community_tone = layer_tones.get("社区讨论", "偏中性")

        if market_signal == "偏弱" and (news_tone == "偏积极" or community_tone == "偏积极"):
            checks.append("市场价格表现偏弱，但外部舆情或社区讨论仍偏积极，需警惕情绪先行而基本面尚未验证。")
        if market_signal == "偏强" and community_tone == "偏谨慎":
            checks.append("价格表现相对更强，但社区层仍偏谨慎，可能意味着短期回调预期或估值分歧扩大。")
        if policy_tone == "偏积极" and news_tone == "偏中性":
            checks.append("政策层支持信号较强，但新闻层尚未形成同步扩散，说明政策预期可能仍处在传导早期。")
        if len(artifact.scorecard_rows) < 2:
            checks.append("财务评分卡覆盖标的较少，横向比较的稳健性有限。")
        if not checks:
            checks.append("当前未发现明显的跨章节冲突，财务、政策与情绪层信号基本一致。")
        return checks

    def _classify_tone(self, item: CollectedItem, layer: str) -> str:
        positive_tokens = ["增长", "创新", "合作", "突破", "提升", "利好", "赋能", "鼓励", "支持", "bullish", "beat", "upgrade", "strong"]
        negative_tokens = ["风险", "争议", "处罚", "下降", "压力", "监管", "规范", "限制", "担忧", "bearish", "lawsuit", "downgrade", "pressure"]
        text = normalize_text(item.title + " " + item.summary + " " + item.content[:1000])
        explicit = str(item.metadata.get("sentiment", "")).strip().lower()
        pos = sum(1 for token in positive_tokens if token.lower() in text)
        neg = sum(1 for token in negative_tokens if token.lower() in text)
        if explicit == "bullish" or pos > neg:
            return "偏积极"
        if explicit == "bearish" or neg > pos:
            return "偏谨慎"
        if layer == "policy" and ("通知" in item.title or "行动计划" in item.title or "实施方案" in item.title):
            return "偏积极"
        return "偏中性"

    def _extract_risks(self, qualitative: list[CollectedItem], quantitative: list[CollectedItem]) -> list[str]:
        risks: list[str] = []
        risk_tokens = ["risk", "uncertainty", "监管", "政策", "volatility", "competition", "constraint", "barrier", "挑战"]
        for item in qualitative + quantitative:
            text = normalize_text(item.content)
            if any(token.lower() in text for token in risk_tokens):
                risks.append(f"{item.title} 暗示了政策、竞争或执行不确定性。")
        return risks[:8] or ["当前证据未充分覆盖风险侧，建议补充政策、宏观和竞争对手信息。"]

    def _extract_opportunities(self, qualitative: list[CollectedItem], quantitative: list[CollectedItem]) -> list[str]:
        opps: list[str] = []
        opp_tokens = ["growth", "创新", "opportunity", "performance", "adoption", "efficiency", "升级", "resilience"]
        for item in qualitative + quantitative:
            text = normalize_text(item.content)
            if any(token.lower() in text for token in opp_tokens):
                opps.append(f"{item.title} 提供了效率提升、创新扩散或竞争优势相关信号。")
        return opps[:8] or ["当前证据更偏基础信息，机会判断仍需补充市场与财务数据。"]

    def _build_scenarios(self, task: ResearchTask, artifact: AnalysisArtifact) -> list[str]:
        return [
            f"基准情景：{task.task} 延续当前趋势，行业变化主要由既有驱动因素推动。",
            f"乐观情景：若政策、技术扩散和组织能力协同增强，{task.task} 的渗透速度可能加快。",
            f"谨慎情景：若监管、成本或竞争压力抬升，{task.task} 相关投入回报可能阶段性承压。",
        ]

    def _build_evidence_map(
        self,
        qualitative: list[CollectedItem],
        quantitative: list[CollectedItem],
        news_items: list[CollectedItem],
        policy_items: list[CollectedItem],
        community_items: list[CollectedItem],
        macro_items: list[CollectedItem],
    ) -> dict[str, list[str]]:
        return {
            "qualitative": [item.title for item in qualitative[:10]],
            "quantitative": [item.title for item in quantitative[:10]],
            "news": [item.title for item in news_items[:10]],
            "policy": [item.title for item in policy_items[:10]],
            "community": [item.title for item in community_items[:10]],
            "macro": [item.title for item in macro_items[:10]],
        }

    def _generate_charts(
        self,
        qualitative: list[CollectedItem],
        quantitative: list[CollectedItem],
        news_items: list[CollectedItem],
        policy_items: list[CollectedItem],
        community_items: list[CollectedItem],
        macro_items: list[CollectedItem],
    ) -> list[str]:
        chart_dir = self.outdir / "charts"
        chart_dir.mkdir(parents=True, exist_ok=True)
        chart_paths: list[str] = []

        source_counter = Counter(
            item.source_name for item in qualitative + quantitative + news_items + policy_items + community_items + macro_items
        )
        if source_counter:
            fig, ax = plt.subplots(figsize=(8, 4.5))
            labels = list(source_counter.keys())[: int(self.config.workflow.get("chart_top_n", 8))]
            values = [source_counter[label] for label in labels]
            ax.bar(labels, values, color="#5470C6")
            ax.set_title("证据来源分布")
            ax.set_ylabel("数量")
            plt.xticks(rotation=20, ha="right")
            plt.tight_layout()
            source_chart = chart_dir / "source_mix.png"
            fig.savefig(source_chart, dpi=160)
            plt.close(fig)
            chart_paths.append(str(source_chart))

        kline_items = [item for item in quantitative if item.metadata.get("dataset_type") == "kline"]
        if kline_items:
            fig, ax = plt.subplots(figsize=(8.5, 4.8))
            plotted = 0
            for item in kline_items[:4]:
                try:
                    df = pd.read_csv(StringIO(item.content))
                except Exception:
                    continue
                if "date" not in df.columns or "close" not in df.columns:
                    continue
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                df = df.dropna(subset=["close"]).tail(40)
                if df.empty:
                    continue
                base = float(df["close"].iloc[0])
                if not base:
                    continue
                norm = df["close"] / base * 100
                ax.plot(df["date"], norm, marker="o", linewidth=1.3, label=item.metadata.get("symbol", item.title))
                plotted += 1
            if plotted:
                ax.set_title("标准化价格对比")
                ax.set_ylabel("基准=100")
                ax.tick_params(axis="x", rotation=25)
                ax.legend()
                plt.tight_layout()
                out = chart_dir / "normalized_price_comparison.png"
                fig.savefig(out, dpi=160)
                plt.close(fig)
                chart_paths.append(str(out))

        for item in quantitative:
            try:
                df = pd.read_csv(StringIO(item.content))
            except Exception:
                continue
            numeric_cols = [col for col in df.columns if pd.to_numeric(df[col], errors="coerce").notna().sum() > 0]
            if len(numeric_cols) >= 1:
                plot_col = numeric_cols[0]
                fig, ax = plt.subplots(figsize=(8, 4.5))
                pd.to_numeric(df[plot_col], errors="coerce").tail(15).plot(ax=ax, marker="o", color="#91CC75")
                ax.set_title(item.title[:60])
                ax.set_ylabel(plot_col)
                plt.tight_layout()
                safe_name = re_safe_name(item.title)
                out = chart_dir / f"{safe_name}.png"
                fig.savefig(out, dpi=160)
                plt.close(fig)
                chart_paths.append(str(out))
                break
        return chart_paths


class AggregatorAgent:
    def __init__(self, config: WorkflowConfig, trace_path: Path, llm: OpenAICompatibleLLM) -> None:
        self.config = config
        self.trace_path = trace_path
        self.llm = llm

    def aggregate(
        self,
        task: ResearchTask,
        plan: SearchPlan,
        items: list[CollectedItem],
        analysis: AnalysisArtifact,
    ) -> ReportArtifact:
        ranked_references = sorted(
            items,
            key=lambda item: float(item.metadata.get("relevance_score", item.score)),
            reverse=True,
        )
        references = [f"- {item.title} ({item.source_name})" for item in ranked_references[:15]]
        markdown = self._llm_report(task, plan, items, analysis) or self._fallback_report(task, plan, items, analysis)
        sections = {
            "executive_summary": "执行摘要",
            "industry_status": "行业现状",
            "quantitative": "财务比对与量化信号",
            "macro": "宏观影响",
            "news": "新闻舆情",
            "policy": "政策动向",
            "community": "社区讨论",
            "swot": "SWOT 与金字塔结论",
            "scenarios": "未来情景",
        }
        artifact = ReportArtifact(
            title=f"{task.task} 深度研究报告",
            markdown=markdown,
            sections=sections,
            references=references,
            chart_paths=analysis.chart_paths,
        )
        log_trace(self.trace_path, "Aggregator", "aggregate", "ok", references=len(references))
        return artifact

    def _llm_report(
        self,
        task: ResearchTask,
        plan: SearchPlan,
        items: list[CollectedItem],
        analysis: AnalysisArtifact,
    ) -> str:
        if not self.llm.enabled:
            return ""
        context_blocks = []
        max_chars = int(self.config.workflow.get("max_context_chars", DEFAULT_WORKFLOW["max_context_chars"]))
        total = 0
        for item in items[:14]:
            block = f"[{item.source_name}] {item.title}\nsummary={item.summary}\ncontent={item.content[:1200]}"
            if context_blocks and total + len(block) > max_chars:
                break
            context_blocks.append(block)
            total += len(block)
        prompt = (
            "请把以下证据组织成一篇专业级行业研究报告。要求使用 MECE、SWOT、金字塔结论法，"
            "并保证跨章节结论一致。必须覆盖：执行摘要、行业现状、财务比对与量化信号、宏观影响、新闻舆情、政策动向、社区讨论、"
            "风险与机会、未来情景、跟踪指标。若某类数据不足要明确说明，不允许编造。\n\n"
            f"任务：{task.task}\n"
            f"关注重点：{'；'.join(plan.report_focus)}\n"
            f"分析摘要：{json.dumps(asdict(analysis), ensure_ascii=False)[:6000]}\n\n"
            + "\n\n".join(context_blocks)
        )
        return self.llm.chat(
            [
                {"role": "system", "content": "你是一名严谨的行业研究总监，输出需结构化、可追溯、结论先行。"},
                {"role": "user", "content": prompt},
            ],
            max_tokens=4200,
        )

    def _fallback_report(
        self,
        task: ResearchTask,
        plan: SearchPlan,
        items: list[CollectedItem],
        analysis: AnalysisArtifact,
    ) -> str:
        ranked_items = sorted(
            items,
            key=lambda item: float(item.metadata.get("relevance_score", item.score)),
            reverse=True,
        )
        references = [f"- {item.title} ({item.source_name})" for item in ranked_items[:12]]
        chart_lines = [f"- ![]({path})" for path in analysis.chart_paths]
        mece_points = [
            "需求侧变化",
            "供给侧能力",
            "政策与制度环境",
            "竞争与替代压力",
        ]
        swot_strengths = analysis.opportunities[:3]
        swot_risks = analysis.risks[:3]
        summary_points = build_summary_points(analysis)
        scorecard_table = markdown_table(
            ["标的", "市场", "价格", "区间涨跌幅", "活跃度", "市值", "PE", "ROE", "净利率", "营收", "净利润"],
            analysis.scorecard_rows,
        )
        sentiment_table = markdown_table(
            ["层级", "偏积极", "偏中性", "偏谨慎", "主导情绪", "代表证据"],
            analysis.sentiment_dashboard,
        )
        risk_matrix = build_risk_matrix(analysis)
        lines = [
            f"# {task.task} 深度研究报告",
            "",
            "## 执行摘要",
            f"围绕“{task.task}”，本轮工作流由 Orchestrator、Searcher、Collector、Analyst、Aggregator 五个智能体串联完成。"
            "当前结论优先基于已收集文献、本地结构化数据和文本材料生成，强调可追溯性、证据相关性与跨章节一致性。",
            "",
            "## 金字塔结论",
            f"核心判断：{task.task} 的研究与行业洞察目前主要由 {'、'.join(mece_points[:3])} 共同驱动，"
            "但结构化金融数据覆盖度仍决定了报告深度上限。",
            "",
            "## 结论摘要页",
        ]
        for point in summary_points:
            lines.append(f"- {point}")
        lines.extend(["", "## 核心评分卡"])
        if scorecard_table:
            lines.extend(scorecard_table)
        else:
            lines.append("- 当前还未形成可读的公司评分卡。")
        lines.extend([
            "",
            "## 行业现状",
        ])
        for finding in analysis.qualitative_findings[:6]:
            lines.append(f"- {finding}")
        lines.extend(["", "## 财务比对与量化信号"])
        for finding in analysis.financial_comparison[:8]:
            lines.append(f"- {finding}")
        for finding in analysis.quantitative_findings[:4]:
            lines.append(f"- {finding}")
        lines.extend(["", "## 宏观影响"])
        for finding in [item for item in analysis.quantitative_findings if "中国" in item or "宏观" in item][:4]:
            lines.append(f"- {finding}")
        if lines[-1] == "## 宏观影响":
            lines.append("- 当前宏观数据较少，建议进一步接入更多利率、就业、投资与行业景气指标。")
        lines.extend(["", "## 新闻舆情"])
        for finding in analysis.sentiment_findings[:6]:
            lines.append(f"- {finding}")
        lines.extend(["", "## 政策动向"])
        for finding in analysis.policy_findings[:6]:
            lines.append(f"- {finding}")
        lines.extend(["", "## 社区讨论"])
        for finding in analysis.community_findings[:6]:
            lines.append(f"- {finding}")
        lines.extend(["", "## 三层情绪总览"])
        if sentiment_table:
            lines.extend(sentiment_table)
        else:
            lines.append("- 当前还未形成稳定的情绪总览。")
        lines.extend(["", "## 一致性校验"])
        for finding in analysis.consistency_checks[:6]:
            lines.append(f"- {finding}")
        lines.extend(["", "## MECE 分析框架"])
        for point in mece_points:
            lines.append(f"- {point}")
        lines.extend(["", "## SWOT"])
        lines.append("- 优势： " + ("；".join(swot_strengths) if swot_strengths else "当前机会信号有限"))
        lines.append("- 劣势： 结构化行情、财报与宏观数据接入尚不完整")
        lines.append("- 机会： " + ("；".join(analysis.opportunities[:3]) if analysis.opportunities else "待补充更多增长信号"))
        lines.append("- 威胁： " + ("；".join(swot_risks) if swot_risks else "待补充风险证据"))
        lines.extend(["", "## 风险矩阵"])
        if risk_matrix:
            lines.extend(risk_matrix)
        else:
            lines.append("- 当前尚未形成稳定的风险矩阵。")
        lines.extend(["", "## 风险与机会"])
        for item in analysis.risks[:4]:
            lines.append(f"- 风险：{item}")
        for item in analysis.opportunities[:4]:
            lines.append(f"- 机会：{item}")
        lines.extend(["", "## 重点证据"])
        for layer, evidence_list in analysis.priority_evidence.items():
            lines.append(f"- {layer}： " + ("；".join(evidence_list[:3]) if evidence_list else "暂无"))
        lines.extend(["", "## 未来情景"])
        for item in analysis.scenarios:
            lines.append(f"- {item}")
        lines.extend(["", "## 跟踪指标"])
        for metric in task.metrics or ["收入增速", "利润率", "研发投入", "政策变化", "舆情情绪"]:
            lines.append(f"- {metric}")
        if chart_lines:
            lines.extend(["", "## 图表"])
            lines.extend(chart_lines)
        lines.extend(["", "## 参考来源"])
        lines.extend(references or ["- 当前没有可引用来源。"])
        return "\n".join(lines) + "\n"


class OrchestratorAgent:
    def __init__(self, trace_path: Path, workflow_settings: dict[str, Any]) -> None:
        self.trace_path = trace_path
        self.workflow_settings = workflow_settings

    def dag(self, task: ResearchTask) -> list[dict[str, Any]]:
        nodes = [{"agent": "Orchestrator", "step": "task_decompose", "depends_on": []}]
        last_step = "task_decompose"
        if self.workflow_settings.get("enable_searcher", True):
            nodes.append({"agent": "Searcher", "step": "search_plan", "depends_on": [last_step]})
            last_step = "search_plan"
        if self.workflow_settings.get("enable_collector", True):
            nodes.append({"agent": "Collector", "step": "data_collect", "depends_on": [last_step]})
            last_step = "data_collect"
        if self.workflow_settings.get("enable_analyst", True):
            nodes.append({"agent": "Analyst", "step": "qual_quant_analysis", "depends_on": [last_step]})
            last_step = "qual_quant_analysis"
        if self.workflow_settings.get("enable_aggregator", True):
            nodes.append({"agent": "Aggregator", "step": "report_generate", "depends_on": [last_step]})
        log_trace(self.trace_path, "Orchestrator", "dag", "ok", nodes=nodes, mode=task.mode)
        return nodes


class DeepResearchWorkflow:
    def __init__(self, config: WorkflowConfig) -> None:
        self.config = config
        self.outdir = Path(config.outdir).expanduser().resolve()
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.trace_path = Path(config.trace_path).expanduser().resolve()
        self.trace_path.parent.mkdir(parents=True, exist_ok=True)
        self.memory = WorkflowMemory(Path(config.memory_path).expanduser().resolve())
        self.llm = OpenAICompatibleLLM(config.llm)
        self.workflow_settings = {**DEFAULT_WORKFLOW, **(config.workflow or {})}
        self.orchestrator = OrchestratorAgent(self.trace_path, self.workflow_settings)
        self.searcher = SearcherAgent(self.trace_path)
        self.collector = CollectorAgent(config, self.trace_path)
        self.analyst = AnalystAgent(config, self.trace_path, self.outdir)
        self.aggregator = AggregatorAgent(config, self.trace_path, self.llm)

    def run(self, task: ResearchTask) -> dict[str, Any]:
        dag = self.orchestrator.dag(task)
        if self.workflow_settings.get("enable_searcher", True):
            plan = self.searcher.run(task)
        else:
            plan = SearchPlan(
                task=task.task,
                intent=task.mode,
                keywords=task.keywords or tokenize(task.task),
                structured_targets=["market_data"],
                unstructured_targets=["literature"],
                required_agents=["orchestrator", "collector", "analyst", "aggregator"],
                report_focus=["执行摘要", "风险与机会", "未来情景"],
            )
            log_trace(self.trace_path, "Searcher", "skipped", "ok", keywords=plan.keywords)

        if self.workflow_settings.get("enable_collector", True):
            items = self.collector.collect(plan, task)
        else:
            items = []
            log_trace(self.trace_path, "Collector", "skipped", "ok")

        if self.workflow_settings.get("enable_analyst", True):
            analysis = self.analyst.analyze(task, plan, items)
        else:
            analysis = AnalysisArtifact(notes=["分析模块已在工作流配置中关闭。"])
            log_trace(self.trace_path, "Analyst", "skipped", "ok")

        if self.workflow_settings.get("enable_aggregator", True):
            report = self.aggregator.aggregate(task, plan, items, analysis)
        else:
            report = ReportArtifact(
                title=f"{task.task} 深度研究报告",
                markdown="# 报告汇总模块已关闭\n",
            )
            log_trace(self.trace_path, "Aggregator", "skipped", "ok")

        output_name = task.output_name or re_safe_name(task.task)
        report_path = self.outdir / f"{output_name}_report.md"
        payload_path = self.outdir / f"{output_name}_payload.json"
        report_path.write_text(report.markdown, encoding="utf-8")
        payload = {
            "task": asdict(task),
            "dag": dag,
            "plan": asdict(plan),
            "items": [asdict(item) for item in items],
            "analysis": asdict(analysis),
            "report": asdict(report),
        }
        save_json(payload_path, payload)

        memory_data = self.memory.update_after_run(
            task,
            {
                "report_path": str(report_path),
                "payload_path": str(payload_path),
                "highlights": analysis.qualitative_findings[:3] + analysis.quantitative_findings[:3],
            },
        )
        summary = {
            "report_path": str(report_path),
            "payload_path": str(payload_path),
            "trace_path": str(self.trace_path),
            "memory_path": str(self.memory.path),
            "items_collected": len(items),
            "charts": report.chart_paths,
            "memory_highlights": memory_data.get("report_highlights", [])[-5:],
        }
        log_trace(self.trace_path, "Orchestrator", "complete", "ok", summary=summary)
        return summary


def re_safe_name(text: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in text)[:80].strip("_") or "报告"


def format_large_number(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return str(value or "暂无")
    abs_number = abs(number)
    if abs_number >= 1_000_000_000_000:
        return f"{number / 1_000_000_000_000:.2f}万亿"
    if abs_number >= 100_000_000:
        return f"{number / 100_000_000:.2f}亿"
    if abs_number >= 10_000:
        return f"{number / 10_000:.2f}万"
    return f"{number:.2f}"


def percent_or_value(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return str(value or "暂无")
    if -5.0 <= number <= 5.0:
        return f"{number * 100:.2f}%"
    return f"{number:.2f}"


def format_decimal(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value or "暂无")


def normalize_metric_display(value: Any, fallback: str = "暂无") -> str:
    try:
        number = float(value)
    except Exception:
        return fallback
    if -5.0 <= number <= 5.0:
        return f"{number * 100:.2f}%"
    return f"{number:.2f}"


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        text = str(value).replace("%", "").replace(",", "").strip()
        return float(text)
    except Exception:
        return default


def extract_focus_terms(text: str) -> list[str]:
    stop_terms = {"比较", "研究", "行业", "平台", "生态", "资本", "市场", "表现", "差异", "中的", "以及", "影响"}
    return [token for token in tokenize(text) if token not in stop_terms and len(token) >= 2]


def infer_market_label(symbol: str) -> str:
    upper = symbol.upper()
    if upper.endswith(".HK"):
        return "HK"
    if upper.startswith(("SH.", "SZ.")) or upper.isdigit():
        return "CN"
    return "US"


def aggregate_market_signal(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "中性"
    values = [safe_float(row.get("区间涨跌幅")) for row in rows if row.get("区间涨跌幅") not in {"", "NA", "暂无"}]
    if not values:
        return "中性"
    avg_return = sum(values) / len(values)
    if avg_return >= 5:
        return "偏强"
    if avg_return <= -5:
        return "偏弱"
    return "中性"


def markdown_table(columns: list[str], rows: list[dict[str, str]]) -> list[str]:
    if not rows:
        return []
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, separator]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(column, "暂无")) for column in columns) + " |")
    return lines


def build_summary_points(analysis: AnalysisArtifact) -> list[str]:
    points: list[str] = []
    if analysis.scorecard_rows:
        leader = analysis.scorecard_rows[0]
        points.append(
            f"财务领先标的暂为 {leader.get('标的', '暂无')}，区间涨跌幅 {leader.get('区间涨跌幅', '暂无')}，ROE {leader.get('ROE', '暂无')}。"
        )
    dashboard = {row["层级"]: row for row in analysis.sentiment_dashboard}
    for layer in ["新闻舆情", "政策动向", "社区讨论"]:
        row = dashboard.get(layer)
        if row:
            points.append(f"{layer} 当前主导情绪为 {row.get('主导情绪', '偏中性')}，代表证据是 {row.get('代表证据', '暂无')}。")
    if analysis.consistency_checks:
        points.append(f"一致性检查提示：{analysis.consistency_checks[0]}")
    return points[:5] or ["当前还未形成稳定的摘要页结论。"]


def build_risk_matrix(analysis: AnalysisArtifact) -> list[str]:
    rows: list[dict[str, str]] = []
    for risk in analysis.risks[:4]:
        rows.append(
            {
                "风险项": risk[:40],
                "可能性": "中",
                "影响": "中高" if ("政策" in risk or "竞争" in risk) else "中",
                "证据": truncate_text(risk, 54),
            }
        )
    for opportunity in analysis.opportunities[:2]:
        rows.append(
            {
                "风险项": truncate_text(opportunity.replace("提供了", "机会侧信号："), 40),
                "可能性": "中",
                "影响": "正向",
                "证据": truncate_text(opportunity, 54),
            }
        )
    return markdown_table(["风险项", "可能性", "影响", "证据"], rows)


def truncate_text(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"
