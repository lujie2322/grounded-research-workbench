from __future__ import annotations

import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any
from urllib.parse import quote

import fitz
import pandas as pd
import requests
from bs4 import BeautifulSoup

from .models import CollectedItem
from .utils import normalize_text, score_text


DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0"}


def request_text(url: str, *, params: dict[str, Any] | None = None, timeout: int = 30) -> str:
    response = requests.get(url, params=params, timeout=timeout, headers=DEFAULT_HEADERS)
    response.raise_for_status()
    return response.text


def strip_html(text: str) -> str:
    if not text:
        return ""
    return " ".join(BeautifulSoup(text, "html.parser").get_text(" ", strip=True).split())


def normalize_a_share_symbol(symbol: str) -> str:
    symbol = symbol.strip()
    if symbol.startswith(("sh.", "sz.")):
        return symbol
    if len(symbol) == 6 and symbol.isdigit():
        return ("sh." if symbol.startswith(("5", "6", "9")) else "sz.") + symbol
    return symbol


def plain_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if "." in normalized:
        left, right = normalized.split(".", 1)
        if right.lower() in {"hk", "us", "sh", "sz"}:
            return left.strip()
        return right.strip()
    return normalized


def detect_symbol_market(symbol: str) -> str:
    normalized = symbol.strip()
    lowered = normalized.lower()
    if lowered.startswith(("sh.", "sz.")):
        return "cn"
    if lowered.endswith((".sh", ".sz")):
        return "cn"
    if len(normalized) == 6 and normalized.isdigit():
        return "cn"
    if lowered.startswith("hk"):
        return "hk"
    if lowered.endswith(".hk"):
        return "hk"
    base = lowered.split(".")[0]
    if base.isdigit() and 1 <= len(base) <= 5:
        return "hk"
    return "us"


def normalize_yahoo_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    market = detect_symbol_market(normalized)
    if market == "hk":
        digits = re.sub(r"[^0-9]", "", normalized)
        digits = (digits.lstrip("0") or "0").zfill(4)
        return f"{digits}.HK"
    if market == "cn":
        return plain_symbol(normalized)
    return normalized.split(".")[0]


def normalize_eastmoney_symbol(symbol: str) -> str:
    normalized = symbol.strip().lower()
    market = detect_symbol_market(normalized)
    if market == "cn":
        return plain_symbol(normalized)
    if market == "hk":
        digits = re.sub(r"[^0-9]", "", normalized)
        digits = (digits.lstrip("0") or "0").zfill(5)
        return f"hk{digits}"
    return normalized.upper().split(".")[0]


class FinanceConnector:
    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        self.settings = settings or {}

    def collect(self, symbols: list[str], metrics: list[str], keywords: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        items.extend(self._collect_baostock(symbols, metrics))
        items.extend(self._collect_yahoo_finance(symbols, metrics))
        if bool(self.settings.get("enable_symbol_financials", False)):
            items.extend(self._collect_akshare_financials(symbols, metrics))
        items.extend(self._collect_akshare_macro(keywords))
        items.extend(self._collect_akshare_stock_news(symbols))
        return items

    def _collect_baostock(self, symbols: list[str], metrics: list[str]) -> list[CollectedItem]:
        try:
            import baostock as bs  # type: ignore
        except Exception:
            return []

        items: list[CollectedItem] = []
        login_result = bs.login()
        if getattr(login_result, "error_code", "1") != "0":
            return []
        try:
            for raw_symbol in symbols:
                symbol = normalize_a_share_symbol(raw_symbol)
                if not symbol.startswith(("sh.", "sz.")):
                    continue

                rs = bs.query_history_k_data_plus(
                    symbol,
                    "date,code,open,high,low,close,volume,amount,turn",
                    start_date=self.settings.get("baostock_start_date", "2024-01-01"),
                    frequency="d",
                    adjustflag="3",
                )
                history_rows = []
                while (rs.error_code == "0") and rs.next():
                    history_rows.append(rs.get_row_data())
                if history_rows:
                    df = pd.DataFrame(history_rows, columns=rs.fields)
                    for col in ["open", "high", "low", "close", "volume", "amount", "turn"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.dropna(subset=["close"])
                    if not df.empty:
                        first_close = float(df["close"].iloc[0])
                        last_close = float(df["close"].iloc[-1])
                        return_pct = ((last_close / first_close) - 1.0) * 100 if first_close else 0.0
                        summary = (
                            f"{symbol} A股日线共 {len(df)} 行，最新收盘 {last_close:.2f}，"
                            f"区间涨跌幅 {return_pct:.2f}%，平均换手率 {df['turn'].mean():.2f}%"
                        )
                        items.append(
                            CollectedItem(
                                item_id=f"baostock:kline:{symbol}",
                                source_type="structured",
                                source_name="Baostock",
                                title=f"{symbol} A股行情",
                                summary=summary,
                                content=df.tail(90).to_csv(index=False),
                                metadata={
                                    "symbol": symbol,
                                    "dataset_type": "kline",
                                    "latest_close": last_close,
                                    "period_return_pct": return_pct,
                                    "avg_turnover": float(df["turn"].mean()),
                                    "metrics": metrics,
                                },
                                score=3.2,
                            )
                        )

                current_year = int(time.strftime("%Y"))
                for year, quarter in [(current_year - 1, 4), (current_year - 1, 3)]:
                    pr = bs.query_profit_data(code=symbol, year=year, quarter=quarter)
                    profit_rows = []
                    while (pr.error_code == "0") and pr.next():
                        profit_rows.append(pr.get_row_data())
                    if profit_rows:
                        profit_df = pd.DataFrame(profit_rows, columns=pr.fields)
                        summary = f"{symbol} 盈利能力数据，最新期 {profit_df.iloc[0].get('statDate', '')}"
                        items.append(
                            CollectedItem(
                                item_id=f"baostock:profit:{symbol}:{year}Q{quarter}",
                                source_type="structured",
                                source_name="Baostock",
                                title=f"{symbol} 盈利数据",
                                summary=summary,
                                content=profit_df.to_csv(index=False),
                                metadata={"symbol": symbol, "dataset_type": "profit", "year": year, "quarter": quarter},
                                score=3.0,
                            )
                        )
                        break

                for year, quarter in [(current_year - 1, 4), (current_year - 1, 3)]:
                    dp = bs.query_dupont_data(code=symbol, year=year, quarter=quarter)
                    dupont_rows = []
                    while (dp.error_code == "0") and dp.next():
                        dupont_rows.append(dp.get_row_data())
                    if dupont_rows:
                        dupont_df = pd.DataFrame(dupont_rows, columns=dp.fields)
                        summary = f"{symbol} 杜邦分析数据，最新期 {dupont_df.iloc[0].get('statDate', '')}"
                        items.append(
                            CollectedItem(
                                item_id=f"baostock:dupont:{symbol}:{year}Q{quarter}",
                                source_type="structured",
                                source_name="Baostock",
                                title=f"{symbol} 杜邦分析",
                                summary=summary,
                                content=dupont_df.to_csv(index=False),
                                metadata={"symbol": symbol, "dataset_type": "dupont", "year": year, "quarter": quarter},
                                score=2.8,
                            )
                        )
                        break
        finally:
            bs.logout()
        return items

    def _collect_yahoo_finance(self, symbols: list[str], metrics: list[str]) -> list[CollectedItem]:
        try:
            import yfinance as yf  # type: ignore
        except Exception:
            return []

        items: list[CollectedItem] = []
        seen_symbols: set[str] = set()
        history_period = str(self.settings.get("yahoo_history_period", "6mo"))

        for raw_symbol in symbols[:8]:
            market = detect_symbol_market(raw_symbol)
            if market not in {"hk", "us"}:
                continue
            symbol = normalize_yahoo_symbol(raw_symbol)
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            try:
                ticker = yf.Ticker(symbol)
            except Exception:
                continue

            try:
                history_df = ticker.history(period=history_period, interval="1d", auto_adjust=False)
            except Exception:
                history_df = pd.DataFrame()
            if not history_df.empty:
                history_df = history_df.reset_index()
                history_df.columns = [str(col).lower().replace(" ", "_") for col in history_df.columns]
                if "date" in history_df.columns:
                    history_df["date"] = pd.to_datetime(history_df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                for col in ["open", "high", "low", "close", "volume"]:
                    if col in history_df.columns:
                        history_df[col] = pd.to_numeric(history_df[col], errors="coerce")
                history_df = history_df.dropna(subset=["close"])
                if not history_df.empty:
                    first_close = float(history_df["close"].iloc[0])
                    last_close = float(history_df["close"].iloc[-1])
                    return_pct = ((last_close / first_close) - 1.0) * 100 if first_close else 0.0
                    avg_volume = float(pd.to_numeric(history_df.get("volume"), errors="coerce").fillna(0).mean())
                    market_label = "港股" if market == "hk" else "美股"
                    items.append(
                        CollectedItem(
                            item_id=f"yahoo:kline:{symbol}",
                            source_type="structured",
                            source_name="Yahoo Finance",
                            title=f"{symbol} {'港股' if market == 'hk' else '美股'}行情",
                            summary=(
                                f"{symbol} {market_label}日线共 {len(history_df)} 行，最新收盘 {last_close:.2f}，"
                                f"区间涨跌幅 {return_pct:.2f}%，平均成交量 {avg_volume:.0f}"
                            ),
                            content=history_df.tail(120).to_csv(index=False),
                            metadata={
                                "symbol": symbol,
                                "market": market,
                                "dataset_type": "kline",
                                "latest_close": last_close,
                                "period_return_pct": return_pct,
                                "avg_volume": avg_volume,
                                "metrics": metrics,
                            },
                            score=3.1,
                        )
                    )

            info = {}
            try:
                info = ticker.info or {}
            except Exception:
                info = {}
            if info:
                snapshot = {
                    "symbol": symbol,
                    "short_name": info.get("shortName") or info.get("longName") or symbol,
                    "market": market,
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "currency": info.get("currency"),
                    "current_price": info.get("currentPrice") or info.get("regularMarketPrice"),
                    "market_cap": info.get("marketCap"),
                    "trailing_pe": info.get("trailingPE"),
                    "forward_pe": info.get("forwardPE"),
                    "return_on_equity": info.get("returnOnEquity"),
                    "profit_margin": info.get("profitMargins"),
                    "total_revenue": info.get("totalRevenue"),
                }
                items.append(
                    CollectedItem(
                        item_id=f"yahoo:snapshot:{symbol}",
                        source_type="structured",
                        source_name="Yahoo Finance",
                        title=f"{symbol} 估值快照",
                        summary=(
                            f"{symbol} 快照：市值 {snapshot.get('market_cap') or '暂无'}，"
                            f"当前价 {snapshot.get('current_price') or '暂无'}，"
                            f"ROE {snapshot.get('return_on_equity') or '暂无'}，"
                            f"净利率 {snapshot.get('profit_margin') or '暂无'}"
                        ),
                        content=json.dumps(snapshot, ensure_ascii=False, indent=2),
                        metadata={"symbol": symbol, "market": market, "dataset_type": "equity_snapshot", **snapshot},
                        score=2.9,
                    )
                )

            try:
                financials_df = ticker.financials
            except Exception:
                financials_df = pd.DataFrame()
            if financials_df is None or financials_df.empty:
                try:
                    financials_df = ticker.income_stmt
                except Exception:
                    financials_df = pd.DataFrame()
            if financials_df is not None and not financials_df.empty:
                financials_df = financials_df.copy()
                financials_df.columns = [str(col.date()) if hasattr(col, "date") else str(col) for col in financials_df.columns]
                summary_df = financials_df.head(12).reset_index().rename(columns={"index": "metric"})
                recent_col = next((col for col in summary_df.columns if col != "metric"), "")
                recent_revenue = ""
                recent_net_income = ""
                if recent_col:
                    revenue_row = summary_df[summary_df["metric"].astype(str).str.contains("Revenue", case=False, na=False)]
                    net_income_row = summary_df[
                        summary_df["metric"].astype(str).str.contains("Net Income", case=False, na=False)
                    ]
                    if not revenue_row.empty:
                        recent_revenue = revenue_row.iloc[0].get(recent_col, "")
                    if not net_income_row.empty:
                        recent_net_income = net_income_row.iloc[0].get(recent_col, "")
                items.append(
                    CollectedItem(
                        item_id=f"yahoo:income_statement:{symbol}",
                        source_type="structured",
                        source_name="Yahoo Finance",
                        title=f"{symbol} 利润表",
                        summary=f"{symbol} 利润表摘要，最新报告期 {recent_col or '暂无'}",
                        content=summary_df.to_csv(index=False),
                        metadata={
                            "symbol": symbol,
                            "market": market,
                            "dataset_type": "income_statement",
                            "latest_period": recent_col,
                            "recent_revenue": recent_revenue,
                            "recent_net_income": recent_net_income,
                        },
                        score=2.7,
                    )
                )
        return items

    def _collect_akshare_financials(self, symbols: list[str], metrics: list[str]) -> list[CollectedItem]:
        try:
            import akshare as ak  # type: ignore
        except Exception:
            return []

        items: list[CollectedItem] = []
        for raw_symbol in symbols:
            symbol = plain_symbol(raw_symbol)
            if len(symbol) != 6 or not symbol.isdigit():
                continue

            try:
                abstract_df = ak.stock_financial_abstract(symbol=symbol)
                cols = [col for col in abstract_df.columns if col not in {"选项", "指标"}]
                latest_col = cols[0] if cols else ""
                summary = f"{symbol} 财务摘要，最新报告期 {latest_col}"
                items.append(
                    CollectedItem(
                        item_id=f"akshare:financial_abstract:{symbol}",
                        source_type="structured",
                        source_name="Akshare",
                        title=f"{symbol} 财务摘要",
                        summary=summary,
                        content=abstract_df.head(25).to_csv(index=False),
                        metadata={"symbol": symbol, "dataset_type": "financial_abstract", "latest_col": latest_col},
                        score=2.7,
                    )
                )
            except Exception:
                pass

            try:
                indicator_df = ak.stock_financial_analysis_indicator(symbol=symbol, start_year=str(int(time.strftime("%Y")) - 2))
                summary = f"{symbol} 财务分析指标，共 {len(indicator_df)} 期"
                items.append(
                    CollectedItem(
                        item_id=f"akshare:analysis_indicator:{symbol}",
                        source_type="structured",
                        source_name="Akshare",
                        title=f"{symbol} 财务分析指标",
                        summary=summary,
                        content=indicator_df.head(12).to_csv(index=False),
                        metadata={"symbol": symbol, "dataset_type": "analysis_indicator", "metrics": metrics},
                        score=2.6,
                    )
                )
            except Exception:
                pass
        return items

    def _collect_akshare_macro(self, keywords: list[str]) -> list[CollectedItem]:
        try:
            import akshare as ak  # type: ignore
        except Exception:
            return []

        items: list[CollectedItem] = []
        macro_targets = [
            ("macro_china_cpi", "中国CPI"),
            ("macro_china_gdp_yearly", "中国GDP同比"),
            ("macro_china_lpr", "中国LPR"),
        ]
        keyword_text = " ".join(keywords)
        if keyword_text:
            items.append(
                CollectedItem(
                    item_id="macro:task_context",
                    source_type="macro",
                    source_name="任务上下文",
                    title="任务宏观上下文",
                    summary=f"当前任务关键词：{keyword_text}",
                    content=keyword_text,
                    metadata={"dataset_type": "task_context"},
                    score=0.3,
                )
            )
        for fn_name, label in macro_targets:
            try:
                df = getattr(ak, fn_name)()
            except Exception:
                continue
            items.append(
                CollectedItem(
                    item_id=f"akshare:{fn_name}",
                    source_type="macro",
                    source_name="Akshare",
                    title=label,
                    summary=f"{label} 数据，共 {len(df)} 行",
                    content=df.head(24).to_csv(index=False),
                    metadata={"dataset_type": fn_name},
                    score=1.8,
                )
            )
        return items

    def _collect_akshare_stock_news(self, symbols: list[str]) -> list[CollectedItem]:
        try:
            import akshare as ak  # type: ignore
        except Exception:
            return []

        items: list[CollectedItem] = []
        for raw_symbol in symbols[:5]:
            symbol = plain_symbol(raw_symbol)
            if len(symbol) != 6 or not symbol.isdigit():
                continue
            try:
                df = ak.stock_news_em(symbol=symbol)
            except Exception:
                continue
            for _, row in df.head(8).iterrows():
                title = str(row.get("新闻标题", "")).strip()
                content = str(row.get("新闻内容", "")).strip()
                if not title:
                    continue
                items.append(
                    CollectedItem(
                        item_id=f"akshare:stock_news:{symbol}:{title[:20]}",
                        source_type="news",
                        source_name="Akshare 新闻",
                        title=title,
                        summary=content[:220],
                        content=content[:3000],
                        metadata={
                            "symbol": symbol,
                            "published_at": str(row.get("发布时间", "")),
                            "publisher": str(row.get("文章来源", "")),
                            "url": str(row.get("新闻链接", "")),
                        },
                        score=2.2,
                    )
                )
        return items


class NewsConnector:
    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        self.settings = settings or {}

    def collect(self, queries: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        max_items = int(self.settings.get("max_items", 8))
        seen_titles: set[str] = set()
        for query in queries[:3]:
            if not query.strip():
                continue
            url = f"https://news.google.com/rss/search?q={quote(query)}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            try:
                xml_text = request_text(url)
                root = ET.fromstring(xml_text)
            except Exception:
                continue
            for node in root.findall("./channel/item"):
                title = strip_html(node.findtext("title", default=""))
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)
                description = strip_html(node.findtext("description", default=""))
                items.append(
                    CollectedItem(
                        item_id=node.findtext("link", default=""),
                        source_type="news",
                        source_name="Google 新闻 RSS",
                        title=title,
                        summary=description[:220],
                        content=description[:3000],
                        metadata={
                            "query": query,
                            "published_at": node.findtext("pubDate", default=""),
                            "url": node.findtext("link", default=""),
                        },
                        score=2.0 + score_text(title + " " + description, [query]),
                    )
                )
                if len(items) >= max_items:
                    return items
        return items


class PolicyConnector:
    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        self.settings = settings or {}

    def collect(self, queries: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        max_items = int(self.settings.get("max_items", 8))
        seen_titles: set[str] = set()
        for query in queries[:4]:
            if not query.strip():
                continue
            params = {
                "t": "zhengcelibrary",
                "q": query,
                "timetype": "timeqb",
                "mintime": "",
                "maxtime": "",
                "sort": "score",
                "sortType": 1,
                "searchfield": "title",
                "pcodeJiguan": "",
                "childtype": "",
                "subchildtype": "",
                "tsbq": "",
                "pubtimeyear": "",
                "puborg": "",
                "pcodeYear": "",
                "pcodeNum": "",
                "filetype": "",
                "p": 1,
                "n": max_items,
                "inpro": "",
                "bmfl": "",
                "dup": "",
                "orpro": "",
                "type": "gwyzcwjk",
            }
            try:
                data = requests.get(
                    "https://sousuo.www.gov.cn/search-gov/data",
                    params=params,
                    timeout=30,
                    headers=DEFAULT_HEADERS,
                ).json()
            except Exception:
                continue
            search_vo = data.get("searchVO") or {}
            cat_map = search_vo.get("catMap") or {}
            for category, payload in cat_map.items():
                for row in payload.get("listVO") or []:
                    title = strip_html(str(row.get("title", "")))
                    if not title or title in seen_titles:
                        continue
                    seen_titles.add(title)
                    items.append(
                        CollectedItem(
                            item_id=str(row.get("url", "")),
                            source_type="policy",
                            source_name="国务院政策库",
                            title=title,
                            summary=strip_html(str(row.get("summary", "")))[:260],
                            content=strip_html(str(row.get("summary", "")))[:3000],
                            metadata={
                                "query": query,
                                "category": category,
                                "published_at": row.get("pubtimeStr", ""),
                                "url": row.get("url", ""),
                            },
                            score=2.4 + score_text(title + " " + str(row.get("summary", "")), [query]),
                        )
                    )
                    if len(items) >= max_items:
                        return items
        return items


class CommunityConnector:
    def __init__(self, settings: dict[str, Any] | None = None) -> None:
        self.settings = settings or {}

    def collect(self, symbols: list[str], keywords: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        providers = self.settings.get("providers") or ["eastmoney_guba", "stocktwits"]
        if "eastmoney_guba" in providers:
            items.extend(self._collect_eastmoney_guba(symbols))
        if "stocktwits" in providers:
            items.extend(self._collect_stocktwits(symbols, keywords))
        max_items = int(self.settings.get("max_items", 8))
        deduped: list[CollectedItem] = []
        seen_titles: set[str] = set()
        for item in sorted(items, key=lambda item: item.score, reverse=True):
            normalized_title = normalize_text(item.title)
            if normalized_title in seen_titles:
                continue
            seen_titles.add(normalized_title)
            deduped.append(item)
            if len(deduped) >= max_items:
                break
        return deduped

    def _collect_stocktwits(self, symbols: list[str], keywords: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        per_symbol_limit = int(self.settings.get("per_symbol_limit", 4))
        for raw_symbol in symbols[:6]:
            if detect_symbol_market(raw_symbol) != "us":
                continue
            symbol = normalize_yahoo_symbol(raw_symbol)
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{quote(symbol)}.json"
            try:
                payload = requests.get(url, timeout=25, headers=DEFAULT_HEADERS).json()
            except Exception:
                continue
            for row in payload.get("messages", [])[:per_symbol_limit]:
                body = strip_html(str(row.get("body", "")))
                if not body:
                    continue
                body_upper = body.upper()
                symbol_token = f"${symbol.upper()}"
                if symbol_token not in body_upper and symbol.upper() not in body_upper:
                    continue
                mood = ""
                entities = row.get("entities") or {}
                sentiment = entities.get("sentiment") or {}
                mood = str(sentiment.get("basic", "")).strip()
                title = body[:80]
                score = 1.8 + score_text(body, [symbol] + keywords[:4])
                if mood.lower() == "bullish":
                    score += 0.2
                elif mood.lower() == "bearish":
                    score += 0.2
                items.append(
                    CollectedItem(
                        item_id=f"stocktwits:{row.get('id', title)}",
                        source_type="community",
                        source_name="Stocktwits",
                        title=title,
                        summary=body[:220],
                        content=body[:3000],
                        metadata={
                            "symbol": symbol,
                            "market": "us",
                            "published_at": str(row.get("created_at", "")),
                            "author": ((row.get("user") or {}).get("username", "")),
                            "sentiment": mood,
                            "url": f"https://stocktwits.com/symbol/{symbol}",
                        },
                        score=score,
                    )
                )
        return items

    def _collect_eastmoney_guba(self, symbols: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        per_symbol_limit = int(self.settings.get("per_symbol_limit", 3))
        for raw_symbol in symbols[:6]:
            market = detect_symbol_market(raw_symbol)
            if market not in {"cn", "hk"}:
                continue
            symbol = normalize_eastmoney_symbol(raw_symbol)
            url = f"https://guba.eastmoney.com/list,{symbol}.html"
            try:
                text = request_text(url, timeout=25)
            except Exception:
                continue
            soup = BeautifulSoup(text, "html.parser")
            links: list[tuple[str, str]] = []
            seen_links: set[str] = set()
            marker = f"/news,{symbol},"
            for a in soup.find_all("a", href=True):
                href = str(a.get("href", "")).strip()
                if marker not in href:
                    continue
                title = a.get_text(" ", strip=True)
                if len(title) < 8 or "反诈" in title:
                    continue
                if href in seen_links:
                    continue
                seen_links.add(href)
                links.append((href, title))
                if len(links) >= per_symbol_limit:
                    break
            for href, fallback_title in links:
                full_url = href if href.startswith("http") else f"https://guba.eastmoney.com{href}"
                try:
                    article_html = request_text(full_url, timeout=25)
                except Exception:
                    continue
                article_soup = BeautifulSoup(article_html, "html.parser")
                title = fallback_title
                if article_soup.title:
                    title = article_soup.title.get_text(strip=True).split("_")[0] or fallback_title
                body_node = article_soup.select_one(".newstext")
                body = strip_html(body_node.get_text(" ", strip=True) if body_node else "")
                if not body:
                    continue
                items.append(
                    CollectedItem(
                        item_id=full_url,
                        source_type="community",
                        source_name="东方财富股吧",
                        title=title,
                        summary=body[:220],
                        content=body[:3000],
                        metadata={
                            "symbol": symbol,
                            "market": market,
                            "url": full_url,
                            "provider": "eastmoney_guba",
                        },
                        score=2.0 + score_text(title + " " + body[:1500], [plain_symbol(symbol)]),
                    )
                )
        return items


class LocalStructuredConnector:
    def collect(self, paths: list[str], keywords: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        for raw in paths:
            path = Path(raw).expanduser()
            if not path.exists():
                continue
            try:
                if path.suffix.lower() == ".csv":
                    df = pd.read_csv(path)
                elif path.suffix.lower() in {".xlsx", ".xls"}:
                    df = pd.read_excel(path)
                else:
                    continue
            except Exception:
                continue
            text = f"{path.name}\n{','.join(map(str, df.columns))}"
            score = score_text(text, keywords)
            items.append(
                CollectedItem(
                    item_id=str(path),
                    source_type="structured",
                    source_name="本地结构化数据",
                    title=path.name,
                    summary=f"本地结构化数据，{len(df)} 行，字段：{', '.join(map(str, df.columns[:10]))}",
                    content=df.head(50).to_csv(index=False),
                    metadata={"path": str(path), "rows": len(df), "columns": list(map(str, df.columns)), "dataset_type": "local_structured"},
                    score=score,
                )
            )
        return sorted(items, key=lambda item: item.score, reverse=True)


class LocalTextConnector:
    def collect(self, paths: list[str], keywords: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        excluded_markers = {
            "run_state",
            "workflow_trace",
            "workflow_memory",
            "agent_trace",
            "agent_memory",
            "compact_context",
            "daily_report",
            "industry_report",
            "qa_",
            "payload",
            "builtin_skills",
            "prompts",
        }
        for raw in paths:
            path = Path(raw).expanduser()
            if not path.exists():
                continue
            if path.is_dir():
                candidates = [p for p in path.rglob("*") if p.is_file()]
            else:
                candidates = [path]
            for file_path in candidates:
                if file_path.suffix.lower() not in {".txt", ".md", ".json"}:
                    continue
                if any(marker in file_path.name.lower() for marker in excluded_markers):
                    continue
                try:
                    text = file_path.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    continue
                score = score_text(file_path.name + "\n" + normalize_text(text[:4000]), keywords)
                if score <= 0:
                    continue
                items.append(
                    CollectedItem(
                        item_id=str(file_path),
                        source_type="unstructured",
                        source_name="本地文本",
                        title=file_path.stem,
                        summary=text[:500].replace("\n", " "),
                        content=text[:8000],
                        metadata={"path": str(file_path)},
                        score=score,
                    )
                )
        return sorted(items, key=lambda item: item.score, reverse=True)


class LocalPdfConnector:
    def collect(self, paths: list[str], keywords: list[str]) -> list[CollectedItem]:
        items: list[CollectedItem] = []
        for raw in paths:
            path = Path(raw).expanduser()
            if not path.exists():
                continue
            if path.is_dir():
                candidates = list(path.rglob("*.pdf"))
            elif path.suffix.lower() == ".pdf":
                candidates = [path]
            else:
                candidates = []
            for pdf_path in candidates:
                try:
                    doc = fitz.open(pdf_path)
                    text = "\n".join(doc.load_page(i).get_text() for i in range(min(3, doc.page_count)))
                except Exception:
                    continue
                score = score_text(pdf_path.stem + "\n" + normalize_text(text[:5000]), keywords)
                if score <= 0:
                    continue
                items.append(
                    CollectedItem(
                        item_id=str(pdf_path),
                        source_type="unstructured",
                        source_name="本地 PDF",
                        title=pdf_path.stem,
                        summary=text[:500].replace("\n", " "),
                        content=text[:8000],
                        metadata={"path": str(pdf_path)},
                        score=score,
                    )
                )
        return sorted(items, key=lambda item: item.score, reverse=True)
