#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
import unicodedata
from pathlib import Path

import fitz


ROOT = Path("/Users/jie/Desktop/人工智能+采用+扎根理论/英文文献")
OUT = Path("/Users/jie/Desktop/editor/output/english_ai_open_coding_batch")

HIGH_PATTERNS = [
    r"\bai adoption\b",
    r"artificial intelligence adoption",
    r"\bai implementation\b",
    r"artificial intelligence implementation",
    r"\bai integration\b",
    r"artificial intelligence integration",
    r"\bai assimilation\b",
    r"\bai readiness\b",
    r"implementation ability",
    r"adoption intensity",
    r"drivers and outcomes of ai adoption",
]

MEDIUM_PATTERNS = [
    r"\bartificial intelligence\b.*\bfirm",
    r"\bartificial intelligence\b.*\benterprise",
    r"\bai\b.*\bfirm",
    r"\bai\b.*\benterprise",
    r"\bai\b.*\bperformance",
    r"\bai\b.*\binnovation",
    r"\bai\b.*\bresilience",
    r"\bai\b.*\bcrm",
    r"\bgenerative ai\b.*\benterprise",
]

EXCERPT_CUES = [
    "this study",
    "we explore",
    "we examine",
    "findings",
    "results",
    "we find",
    "reveals",
    "indicated",
    "significant",
    "influence",
    "effect",
    "impact",
    "antecedent",
    "outcome",
    "mechanism",
    "mediating",
    "moderating",
    "boundary",
    "future research",
    "limitation",
    "discussion",
    "conclusion",
    "proposition",
    "hypothesis",
    "heterogeneity",
    "supported",
    "assimilation",
    "antecedents",
    "consequences",
    "readiness",
    "capability",
]


def normalize(text: str) -> str:
    text = text.replace("\x00", " ")
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def safe_slug(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\\/:*?\"<>|]+", "_", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:140] or "untitled"


def classify(name: str) -> tuple[str, str]:
    n = name.lower()
    if any(re.search(p, n) for p in HIGH_PATTERNS):
        return "高", "核心相关"
    if any(re.search(p, n) for p in MEDIUM_PATTERNS):
        return "中", "补充相关"
    return "低", "待议"


def extract_text(pdf: Path) -> str:
    doc = fitz.open(pdf)
    chunks = []
    for i in range(doc.page_count):
        try:
            chunks.append(doc.load_page(i).get_text())
        except Exception:
            continue
    return normalize("\n".join(chunks))


def guess_title(text: str, fallback: str) -> str:
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    for line in lines[:25]:
        if len(line) < 12 or len(line) > 180:
            continue
        if re.search(r"(vol\.|doi|available online|accepted:|published online|keywords|abstract)", line.lower()):
            continue
        if sum(ch.isalpha() for ch in line) < 8:
            continue
        if any(k in line.lower() for k in ["adoption", "artificial intelligence", " ai ", "implementation", "integration"]):
            return line
    return fallback


def guess_authors(text: str, title: str) -> str:
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    for i, line in enumerate(lines[:30]):
        if title[:40] in line:
            for cand in lines[i + 1:i + 4]:
                if len(cand) < 80 and re.search(r"[A-Z][a-z]+", cand) and not any(w in cand.lower() for w in ["abstract", "keywords", "accepted", "published"]):
                    return cand
    for line in lines[:12]:
        if "·" in line or "," in line:
            if re.search(r"[A-Z][a-z]+", line) and not any(w in line.lower() for w in ["vol.", "doi", "abstract"]):
                return line
    return "待补充"


def guess_source(text: str) -> str:
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    for line in lines[:12]:
        if any(k in line.lower() for k in ["journal", "review", "management", "research", "forecasting", "technovation", "letters", "transactions"]):
            if len(line) <= 120:
                return line
    return "待补充"


def guess_year(text: str) -> str:
    m = re.search(r"(20\d{2})", text)
    return m.group(1) if m else "待补充"


def guess_method(text: str) -> str:
    patterns = [
        "survey",
        "structural equation modeling",
        "mixed-methods",
        "mixed methods",
        "case study",
        "multiple case study",
        "qualitative study",
        "semi-structured interviews",
        "regression",
        "fsqca",
        "meta-analysis",
        "panel data",
        "textual analysis",
    ]
    hits = [p for p in patterns if p in text.lower()]
    return "、".join(hits[:5]) if hits else "待补充"


def guess_theory(text: str) -> str:
    patterns = [
        "toe",
        "technology-organization-environment",
        "tam",
        "utaut",
        "dynamic capability",
        "upper echelons",
        "diffusion of innovations",
        "institutional theory",
        "organizational information processing",
        "resource-based view",
        "affordance",
    ]
    hits = [p for p in patterns if p in text.lower()]
    return "、".join(hits[:5]) if hits else "待补充"


def get_section(text: str, starts: list[str], ends: list[str]) -> str:
    lower = text.lower()
    start = -1
    for s in starts:
        idx = lower.find(s.lower())
        if idx != -1 and (start == -1 or idx < start):
            start = idx
    if start == -1:
        return ""
    end = len(text)
    for e in ends:
        idx = lower.find(e.lower(), start + 1)
        if idx != -1 and idx < end:
            end = idx
    return text[start:end]


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?;])\s+", text.replace("\n", " "))
    out = []
    for p in parts:
        p = re.sub(r"\[[^\]]+\]", "", p)
        p = re.sub(r"\s+", " ", p).strip(" -–—*")
        if len(p) < 30:
            continue
        out.append(p)
    return out


def candidate_excerpts(text: str) -> list[str]:
    blocks = []
    abstract = get_section(text, ["Abstract"], ["Keywords", "1 Introduction", "Introduction"])
    if abstract:
        blocks.append(abstract)
    lower = text.lower()
    for marker in [
        "research question",
        "we examine",
        "we explore",
        "this study examines",
        "this study explores",
        "hypothesis",
        "proposition",
        "findings",
        "results",
        "discussion",
        "conclusion",
        "future research",
        "limitations",
        "managerial implications",
    ]:
        idx = lower.find(marker.lower())
        if idx != -1:
            blocks.append(text[idx: min(len(text), idx + 2400)])
    for pattern in [
        r"hypotheses?\s+\d",
        r"proposition[s]?",
        r"mediat\w+",
        r"moderat\w+",
        r"heterogeneity",
        r"boundary condition[s]?",
        r"future research",
        r"limitations?",
        r"discussion",
        r"results show",
        r"we find",
        r"findings reveal",
        r"supported",
    ]:
        for m in re.finditer(pattern, lower):
            start = max(0, m.start() - 300)
            end = min(len(text), m.end() + 1200)
            blocks.append(text[start:end])
    seen = set()
    excerpts = []
    for block in blocks:
        for sent in split_sentences(block):
            low = sent.lower()
            if not any(c in low for c in EXCERPT_CUES):
                continue
            if any(x in low for x in ["usable responses", "sample of", "data were collected", "dataset was compiled", "structural equation modeling was applied", "survey-based data collected", "we used", "regression model"]) and not any(y in low for y in ["influence", "effect", "impact", "mediating", "moderating", "significant", "support", "performance", "adoption"]):
                continue
            if any(x in low for x in ["copyright", "all rights reserved", "author(s)"]):
                continue
            if sent not in seen:
                seen.add(sent)
                excerpts.append(sent)
    return excerpts[:12]


def split_codes(excerpt: str) -> list[str]:
    base = excerpt
    parts = re.split(r";|, and | and | through | by | under | while ", base)
    codes = []
    for p in parts:
        p = re.sub(r"^(this study|we|the findings|results|findings reveal that|results indicate that)\s+", "", p, flags=re.I).strip(" ,.;:")
        if len(p) >= 8 and p not in codes:
            codes.append(p)
    return codes[:4] or [excerpt]


def nature(excerpt: str) -> str:
    low = excerpt.lower()
    if any(x in low for x in ["antecedent", "determinant", "driver", "readiness", "influence the adoption", "adoption is influenced"]):
        return "暂似驱动因素"
    if any(x in low for x in ["performance", "innovation", "resilience", "efficiency", "positive correlation", "effect on", "impact on", "outcome"]):
        return "暂似结果"
    if any(x in low for x in ["moderating", "boundary", "context", "under the moderating effects", "limitation", "future research"]):
        return "暂似条件"
    if any(x in low for x in ["mechanism", "mediating", "through", "path", "assimilation"]):
        return "暂似机制"
    return "暂不确定"


def reason_lines(excerpt: str, n: str) -> list[str]:
    out = [
        "- 该句属于作者对研究问题、变量关系、研究发现、机制解释、条件性表述或未来研究方向的直接陈述，符合初级编码保留范围。",
        "- 当前阶段尽量保留原文术语，如 adoption, implementation, integration, performance, moderation 等，避免过早抽象。",
    ]
    if n == "暂似驱动因素":
        out.append("- 句中主要呈现企业AI采用/实施的影响因素、前提条件或决定因素，因此暂作驱动性弱判断。")
    elif n == "暂似结果":
        out.append("- 句中主要呈现AI采用后的绩效、创新、效率、韧性等后果，因此暂作结果性弱判断。")
    elif n == "暂似条件":
        out.append("- 句中出现调节、情境、边界、局限或 future research 线索，因此暂作条件性弱判断。")
    elif n == "暂似机制":
        out.append("- 句中强调通过何种路径、中介或作用方式产生影响，因此暂作机制性弱判断。")
    else:
        out.append("- 该句与研究主线相关，但更接近研究问题或综合论断，当前不强行归类。")
    return out


def memo_lines(rel: str, excerpt: str) -> list[str]:
    out = [
        "- 需要结合“企业人工智能技术采用”的主线，关注该条编码是否直接涉及企业AI adoption、implementation、integration、assimilation 或应用结果。",
        "- 后续聚焦编码时，可与其他英文和中文文献中的相近概念做合并或区分，观察其是否形成稳定范畴。",
    ]
    if rel == "高":
        out.append("- 该文献与主题高度相关，这条编码通常值得优先纳入下一轮聚焦编码池。")
    elif rel == "中":
        out.append("- 该文献与主题存在补充性关系，建议后续结合上下文再判断保留强度。")
    else:
        out.append("- 该文献与主题关联较弱，建议在下一轮聚焦编码时重点核查是否仅属于扩展讨论。")
    if any(x in excerpt.lower() for x in ["future research", "limitation", "boundary", "context"]):
        out.append("- 这条内容可重点观察其是否提供边界条件或未来研究线索。")
    return out


def build_doc(meta: dict, excerpts: list[str]) -> str:
    lines = [
        "【文献基本信息】",
        f"- 作者：{meta['authors']}",
        f"- 年份：{meta['year']}",
        f"- 题目：{meta['title']}",
        f"- 来源：{meta['source']}",
        "- 研究对象：待补充",
        f"- 研究方法：{meta['method']}",
        f"- 理论基础：{meta['theory']}",
        f"- 与研究主题相关性：{meta['relevance']}",
        f"- 初步判断：{meta['judgment']}",
        "",
        "【初级编码】",
    ]
    summary = []
    for i, excerpt in enumerate(excerpts, 1):
        sid = f"S{i}"
        lines.append(f"{sid} 原文：")
        lines.append(f"“{excerpt}”")
        lines.append("")
        codes = split_codes(excerpt)
        n = nature(excerpt)
        for j, code in enumerate(codes):
            suffix = chr(ord("a") + j) if len(codes) > 1 else ""
            cid = f"C{i}{suffix}"
            lines.append(f"{cid} 初级编码：")
            lines.append(f"- {code}")
            lines.append("")
            lines.append("暂定性质：")
            lines.append(f"- {n}")
            lines.append("")
            lines.append("编码理由：")
            lines.extend(reason_lines(excerpt, n))
            lines.append("")
            lines.append("备忘录 Memo：")
            lines.extend(memo_lines(meta["relevance"], excerpt))
            pending = "是" if meta["relevance"] == "低" else "否"
            lines.append(f"- 是否需要标记为“待议”：{pending}")
            lines.append("")
            summary.append((cid, sid, excerpt[:80], code, n, pending, "英文文献自动正式编码"))
    lines.extend([
        "【初级编码汇总表】",
        "",
        "| 编码编号 | 原文编号 | 原文摘录 | 初级编码 | 暂定性质 | 是否待议 | 备注 |",
        "|---|---|---|---|---|---|---|",
    ])
    for row in summary:
        lines.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} |")
    lines.extend([
        "",
        "【文献级备忘录】",
        "1. 这篇文献最值得保留的初级概念，通常集中在 abstract、findings、discussion、conclusion 和 future research 段落。",
        "2. 与“企业人工智能技术采用”主线最相关的内容，应优先关注企业层面的 adoption、implementation、integration、assimilation、readiness、intensity 及其结果。",
        "3. 下一轮聚焦编码中，应重点观察反复出现的 antecedents、outcomes、mechanisms、boundary conditions 是否形成稳定概念簇。",
        "4. 若文章主要讨论一般数字化、非AI技术或消费者采用，则与主线关系较弱，应谨慎保留。",
        f"5. 基于当前初筛，这篇文献建议暂定为“{meta['judgment']}”，后续可结合全文再微调。",
        "6. 当前文档已保证“原文—初级编码—编码理由—备忘录”链条完整，但仍建议将高相关文献优先做进一步人工精修。",
    ])
    return "\n".join(lines) + "\n"


def main() -> int:
    texts = OUT / "texts"
    finals = OUT / "final_codings_all"
    texts.mkdir(parents=True, exist_ok=True)
    finals.mkdir(parents=True, exist_ok=True)

    index_rows = []
    summary = {"高": 0, "中": 0, "低": 0}
    failures = []

    for pdf in sorted(ROOT.glob("*.pdf")):
        try:
            txt = extract_text(pdf)
            if not txt.strip():
                failures.append({"filename": pdf.name, "reason": "empty_text"})
                continue
            (texts / f"{safe_slug(pdf.stem)}.txt").write_text(txt, encoding="utf-8")
            title = guess_title(txt, pdf.stem)
            authors = guess_authors(txt, title)
            source = guess_source(txt)
            year = guess_year(txt)
            method = guess_method(txt[:15000])
            theory = guess_theory(txt[:15000])
            relevance, judgment = classify(pdf.stem)
            excerpts = candidate_excerpts(txt)
            if not excerpts:
                excerpts = split_sentences(txt[:3000])[:6]
            meta = {
                "filename": pdf.name,
                "title": title,
                "authors": authors,
                "source": source,
                "year": year,
                "method": method,
                "theory": theory,
                "relevance": relevance,
                "judgment": judgment,
                "candidate_count": str(len(excerpts)),
            }
            summary[relevance] += 1
            doc = build_doc(meta, excerpts)
            (finals / f"{safe_slug(pdf.stem)}.md").write_text(doc, encoding="utf-8")
            index_rows.append(meta)
        except Exception as e:
            failures.append({"filename": pdf.name, "reason": str(e)})
            continue

    with (OUT / "index.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["filename", "title", "authors", "source", "year", "method", "theory", "relevance", "judgment", "candidate_count"])
        for row in index_rows:
            w.writerow([row["filename"], row["title"], row["authors"], row["source"], row["year"], row["method"], row["theory"], row["relevance"], row["judgment"], row["candidate_count"]])

    with (OUT / "summary.json").open("w", encoding="utf-8") as f:
        json.dump({"total_pdfs": len(index_rows), "high_relevance": summary["高"], "medium_relevance": summary["中"], "low_relevance": summary["低"], "failures": len(failures)}, f, ensure_ascii=False, indent=2)

    with (OUT / "final_codings_all_index.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["filename", "relevance", "judgment", "candidate_count", "final_doc"])
        for row in index_rows:
            w.writerow([row["filename"], row["relevance"], row["judgment"], row["candidate_count"], str(finals / f"{safe_slug(Path(row['filename']).stem)}.md")])

    with (OUT / "failures.json").open("w", encoding="utf-8") as f:
        json.dump(failures, f, ensure_ascii=False, indent=2)

    print(json.dumps({"total_pdfs": len(index_rows), "high_relevance": summary["高"], "medium_relevance": summary["中"], "low_relevance": summary["低"], "failures": len(failures)}, ensure_ascii=False))
    print(OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
