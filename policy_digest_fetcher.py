from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


DEFAULT_HEADERS = {"User-Agent": "Mozilla/5.0"}
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "output" / "policy_digest"
AI_KEYWORDS = [
    "人工智能",
    "生成式人工智能",
    "大模型",
    "算法",
    "算力",
    "智能制造",
    "智能网联",
    "智能机器人",
]
CORE_POLICY_PATTERNS = [
    "发展规划",
    "行动计划",
    "指导意见",
    "实施意见",
    "管理办法",
    "暂行办法",
    "若干措施",
    "条例",
    "规划",
]
POLICY_PATTERNS = CORE_POLICY_PATTERNS + [
    "通知",
    "方案",
    "规则",
    "公告",
    "政策问答",
]
NEWS_PATTERNS = [
    "发布",
    "解读",
    "答记者问",
    "负责人",
    "公告",
]
KNOWN_CORE_TITLES = {
    "新一代人工智能发展规划",
    "关于加快场景创新以人工智能高水平应用促进经济高质量发展的指导意见",
    "生成式人工智能服务管理暂行办法",
    "“十四五”数字经济发展规划",
}


@dataclass
class PolicyItem:
    item_id: str
    title: str
    summary: str
    url: str
    source_name: str
    source_type: str
    published_at: str = ""
    issuing_body: str = ""
    category: str = ""
    matched_keywords: list[str] | None = None
    is_core: bool = False
    rule_hits: list[str] | None = None


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\x00", " ")).strip().lower()


def strip_html(text: str) -> str:
    if not text:
        return ""
    return " ".join(BeautifulSoup(text, "html.parser").get_text(" ", strip=True).split())


def canonicalize_url(url: str, base_url: str = "") -> str:
    if not url:
        return ""
    joined = urljoin(base_url, url)
    return joined.replace("http://", "https://").strip()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


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


def request_json(url: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = requests.get(url, params=params, timeout=30, headers=DEFAULT_HEADERS)
    response.raise_for_status()
    return response.json()


def request_text(url: str, *, params: dict[str, Any] | None = None) -> str:
    response = requests.get(url, params=params, timeout=30, headers=DEFAULT_HEADERS)
    response.raise_for_status()
    return response.text


def extract_keywords(text: str, keywords: list[str]) -> list[str]:
    haystack = normalize_text(text)
    return [keyword for keyword in keywords if keyword.lower() in haystack]


def is_ai_related(title: str, summary: str, keywords: list[str]) -> bool:
    return bool(extract_keywords(f"{title} {summary}", keywords))


def classify_item(title: str, source_name: str, source_type: str) -> tuple[str, bool, list[str]]:
    rule_hits: list[str] = []
    if source_type == "policy":
        if title in KNOWN_CORE_TITLES:
            rule_hits.append("命中已知核心政策名单")
        for pattern in CORE_POLICY_PATTERNS:
            if pattern in title:
                rule_hits.append(f"命中核心政策模式：{pattern}")
        if title in KNOWN_CORE_TITLES or any(pattern in title for pattern in CORE_POLICY_PATTERNS):
            return "核心政策", True, rule_hits
        if any(pattern in title for pattern in POLICY_PATTERNS):
            for pattern in POLICY_PATTERNS:
                if pattern in title:
                    rule_hits.append(f"命中一般政策模式：{pattern}")
            return "相关政策", False, rule_hits
        if source_name == "国务院政策库":
            rule_hits.append("来自国务院政策库")
            return "相关政策", False, rule_hits
    if any(pattern in title for pattern in NEWS_PATTERNS):
        for pattern in NEWS_PATTERNS:
            if pattern in title:
                rule_hits.append(f"命中新闻 / 解读模式：{pattern}")
        return "政策解读 / 新闻", False, rule_hits
    if source_name == "国家网信办":
        rule_hits.append("来自国家网信办")
    return "相关新闻", False, rule_hits


def dedupe_items(items: list[PolicyItem]) -> list[PolicyItem]:
    deduped: dict[str, PolicyItem] = {}
    for item in items:
        url_key = canonicalize_url(item.url)
        title_key = normalize_text(item.title)
        key = url_key or title_key
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = item
            continue
        if len(item.summary or "") > len(existing.summary or ""):
            deduped[key] = item
    return list(deduped.values())


def fetch_gov_policy_library(queries: list[str], *, pages_per_query: int = 5, page_size: int = 20) -> list[PolicyItem]:
    items: list[PolicyItem] = []
    for query in queries:
        if not query.strip():
            continue
        for page in range(1, pages_per_query + 1):
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
                "p": page,
                "n": page_size,
                "inpro": "",
                "bmfl": "",
                "dup": "",
                "orpro": "",
                "type": "gwyzcwjk",
            }
            try:
                data = request_json("https://sousuo.www.gov.cn/search-gov/data", params=params)
            except Exception:
                break
            search_vo = data.get("searchVO") or {}
            cat_map = search_vo.get("catMap") or {}
            rows_found = 0
            for payload in cat_map.values():
                for row in payload.get("listVO") or []:
                    title = strip_html(str(row.get("title", "")))
                    summary = strip_html(str(row.get("summary", "")))
                    url = canonicalize_url(str(row.get("url", "")), "https://www.gov.cn")
                    if not title or not is_ai_related(title, summary, queries):
                        continue
                    category, is_core, rule_hits = classify_item(title, "国务院政策库", "policy")
                    items.append(
                        PolicyItem(
                            item_id=url or title,
                            title=title,
                            summary=summary[:280],
                            url=url,
                            source_name="国务院政策库",
                            source_type="policy",
                            published_at=str(row.get("pubtimeStr", "")),
                            issuing_body=str(row.get("puborg", "")),
                            category=category,
                            matched_keywords=extract_keywords(f"{title} {summary}", queries),
                            is_core=is_core,
                            rule_hits=rule_hits,
                        )
                    )
                    rows_found += 1
            if rows_found == 0:
                break
    return items


def fetch_page_link_items(
    url: str,
    *,
    source_name: str,
    default_type: str,
    keywords: list[str],
    limit: int = 24,
) -> list[PolicyItem]:
    try:
        html = request_text(url)
    except Exception:
        return []
    soup = BeautifulSoup(html, "html.parser")
    items: list[PolicyItem] = []
    seen_titles: set[str] = set()
    for link in soup.select("a[href]"):
        title = " ".join(link.get_text(" ", strip=True).split())
        href = canonicalize_url(link.get("href", ""), url)
        if not title or not href or len(title) < 6 or title in seen_titles:
            continue
        if "gov.cn" not in href and "cac.gov.cn" not in href:
            continue
        if not is_ai_related(title, "", keywords):
            continue
        seen_titles.add(title)
        category, is_core, rule_hits = classify_item(title, source_name, default_type)
        items.append(
            PolicyItem(
                item_id=href or title,
                title=title,
                summary="",
                url=href,
                source_name=source_name,
                source_type=default_type,
                published_at="",
                issuing_body=source_name,
                category=category,
                matched_keywords=extract_keywords(title, keywords),
                is_core=is_core,
                rule_hits=rule_hits,
            )
        )
        if len(items) >= limit:
            break
    return items


def fetch_gov_recent_news(keywords: list[str]) -> list[PolicyItem]:
    sources = [
        ("https://www.gov.cn/zhengce/index.htm", "中国政府网政策库", "policy"),
        ("https://www.gov.cn/zhengce/jiedu/index.htm", "中国政府网政策解读", "news"),
    ]
    items: list[PolicyItem] = []
    for url, source_name, source_type in sources:
        items.extend(fetch_page_link_items(url, source_name=source_name, default_type=source_type, keywords=keywords))
    return items


def fetch_cac_recent_news(keywords: list[str]) -> list[PolicyItem]:
    items = fetch_page_link_items(
        "https://www.cac.gov.cn/",
        source_name="国家网信办",
        default_type="news",
        keywords=keywords,
        limit=18,
    )
    normalized: list[PolicyItem] = []
    for item in items:
        category, is_core, rule_hits = classify_item(item.title, "国家网信办", "policy" if any(pattern in item.title for pattern in POLICY_PATTERNS) else "news")
        item.source_type = "policy" if any(pattern in item.title for pattern in POLICY_PATTERNS) else "news"
        item.category = category
        item.is_core = is_core
        item.rule_hits = rule_hits
        normalized.append(item)
    return normalized


def build_daily_digest(
    *,
    run_date: str,
    core_policies: list[PolicyItem],
    all_policies: list[PolicyItem],
    news_items: list[PolicyItem],
    new_items: list[PolicyItem],
) -> str:
    lines = [
        f"# 人工智能政策与新闻日报 {run_date}",
        "",
        "## 抓取概览",
        "",
        f"- 核心政策数：{len(core_policies)}",
        f"- 全部政策数：{len(all_policies)}",
        f"- 新闻 / 解读数：{len(news_items)}",
        f"- 今日新增数：{len(new_items)}",
        "",
        "## 今日新增",
        "",
    ]
    if new_items:
        for item in new_items[:20]:
            label = item.category or ("核心政策" if item.is_core else item.source_type)
            lines.append(f"- [{item.title}]({item.url})")
            lines.append(f"  来源：{item.source_name} | 类型：{label} | 日期：{item.published_at or '待补充'}")
    else:
        lines.append("- 今日暂无新增条目。")
    lines.extend(["", "## 核心政策", ""])
    for item in core_policies[:20]:
        lines.append(f"- [{item.title}]({item.url})")
    lines.extend(["", "## 最新新闻 / 解读", ""])
    for item in news_items[:20]:
        lines.append(f"- [{item.title}]({item.url})")
    lines.append("")
    return "\n".join(lines)


def save_table(path: Path, items: list[PolicyItem]) -> None:
    rows = [asdict(item) for item in items]
    if not rows:
        save_json(path.with_suffix(".json"), [])
        return
    import pandas as pd

    dataframe = pd.DataFrame(rows)
    if path.suffix == ".csv":
        dataframe.to_csv(path, index=False)
    else:
        dataframe.to_excel(path, index=False)


def fetch_policy_digest(
    *,
    outdir: Path,
    queries: list[str],
    pages_per_query: int,
    page_size: int,
) -> dict[str, Any]:
    latest_dir = ensure_dir(outdir / "latest")
    history_dir = ensure_dir(outdir / "history" / date.today().isoformat())
    state_path = outdir / "policy_digest_state.json"
    log_path = outdir / "policy_fetch_log.jsonl"
    state = load_json(state_path, {"seen_keys": [], "last_run_at": ""})
    seen_keys = set(state.get("seen_keys", []))

    all_items = dedupe_items(
        fetch_gov_policy_library(queries, pages_per_query=pages_per_query, page_size=page_size)
        + fetch_gov_recent_news(queries)
        + fetch_cac_recent_news(queries)
    )
    policy_items = [item for item in all_items if item.source_type == "policy"]
    news_items = [item for item in all_items if item.source_type != "policy"]
    core_policies = [item for item in policy_items if item.is_core]

    new_items: list[PolicyItem] = []
    current_keys: list[str] = []
    for item in all_items:
        key = canonicalize_url(item.url) or normalize_text(item.title)
        current_keys.append(key)
        if key not in seen_keys:
            new_items.append(item)

    summary = {
        "run_at": datetime.now().isoformat(timespec="seconds"),
        "queries": queries,
        "core_policy_count": len(core_policies),
        "all_policy_count": len(policy_items),
        "news_count": len(news_items),
        "new_item_count": len(new_items),
        "state_path": str(state_path),
        "latest_dir": str(latest_dir),
    }

    save_json(latest_dir / "core_policies.json", [asdict(item) for item in core_policies])
    save_json(latest_dir / "all_policies.json", [asdict(item) for item in policy_items])
    save_json(latest_dir / "news_updates.json", [asdict(item) for item in news_items])
    save_json(latest_dir / "daily_updates.json", [asdict(item) for item in new_items])
    save_json(latest_dir / "summary.json", summary)
    save_json(history_dir / "summary.json", summary)
    save_json(history_dir / "core_policies.json", [asdict(item) for item in core_policies])
    save_json(history_dir / "all_policies.json", [asdict(item) for item in policy_items])
    save_json(history_dir / "news_updates.json", [asdict(item) for item in news_items])
    save_json(history_dir / "daily_updates.json", [asdict(item) for item in new_items])

    digest_markdown = build_daily_digest(
        run_date=date.today().isoformat(),
        core_policies=core_policies,
        all_policies=policy_items,
        news_items=news_items,
        new_items=new_items,
    )
    (latest_dir / f"daily_digest_{date.today().isoformat()}.md").write_text(digest_markdown, encoding="utf-8")
    (history_dir / f"daily_digest_{date.today().isoformat()}.md").write_text(digest_markdown, encoding="utf-8")
    save_table(latest_dir / "core_policies.csv", core_policies)
    save_table(latest_dir / "all_policies.csv", policy_items)
    save_table(latest_dir / "news_updates.csv", news_items)

    state["seen_keys"] = sorted(set(current_keys).union(seen_keys))
    state["last_run_at"] = summary["run_at"]
    save_json(state_path, state)

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(summary, ensure_ascii=False) + "\n")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="抓取中国人工智能相关政策与官方新闻。")
    parser.add_argument("--outdir", default=str(DEFAULT_OUTPUT_DIR), help="输出目录，默认 output/policy_digest")
    parser.add_argument("--pages-per-query", type=int, default=4, help="国务院政策库每个关键词抓取页数")
    parser.add_argument("--page-size", type=int, default=20, help="每页抓取条数")
    parser.add_argument("--queries", nargs="*", default=AI_KEYWORDS, help="抓取关键词列表")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir).expanduser().resolve()
    summary = fetch_policy_digest(
        outdir=outdir,
        queries=list(dict.fromkeys(args.queries)),
        pages_per_query=int(args.pages_per_query),
        page_size=int(args.page_size),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
