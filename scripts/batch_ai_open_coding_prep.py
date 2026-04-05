#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import fitz


HIGH_PATTERNS = [
    r"人工智能.*采纳",
    r"AI.*采纳",
    r"人工智能.*采用",
    r"AI.*采用",
    r"人工智能.*应用",
    r"AI.*应用",
    r"人工智能.*实施",
    r"AI.*实施",
    r"人工智能.*部署",
    r"AI.*部署",
    r"人工智能.*整合",
    r"人工智能.*嵌入",
    r"人工智能.*同化",
]

MEDIUM_PATTERNS = [
    r"生成式人工智能.*企业",
    r"人工智能.*企业",
    r"AI.*企业",
    r"机器人应用.*企业",
    r"工业机器人.*企业",
]

CANDIDATE_CUES = [
    "研究发现",
    "结果表明",
    "本文发现",
    "发现",
    "影响",
    "有助于",
    "提升",
    "抑制",
    "促进",
    "驱动",
    "机制",
    "中介",
    "调节",
    "异质性",
    "条件",
    "情境",
    "命题",
    "假设",
    "研究问题",
    "未来研究",
    "结论",
    "讨论",
    "构建",
    "路径",
]

EXCLUDE_CUES = [
    "基金项目",
    "收稿日期",
    "作者简介",
    "参考文献",
    "文献综述",
    "中图分类号",
    "文献标识码",
    "DOI",
    "关键词",
    "编者按",
    "客座编辑",
]


@dataclass
class PaperRecord:
    filename: str
    relevance: str
    judgment: str
    page_count: int
    title_guess: str
    authors_guess: str
    year_guess: str
    method_guess: str
    theory_guess: str
    candidate_count: int


def safe_slug(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\\/:*?\"<>|]+", "_", text)
    text = re.sub(r"\s+", "_", text.strip())
    return text[:120] or "untitled"


def classify_relevance(name: str) -> tuple[str, str]:
    compact = "".join(name.split())
    if any(re.search(p, compact, re.I) for p in HIGH_PATTERNS):
        return "高", "核心相关"
    if any(re.search(p, compact, re.I) for p in MEDIUM_PATTERNS):
        return "中", "补充相关"
    return "低", "待议"


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def guess_year(text: str) -> str:
    m = re.search(r"(20\d{2})", text)
    return m.group(1) if m else ""


def guess_title(text: str, fallback: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for idx, line in enumerate(lines[:30]):
        if len(line) < 8:
            continue
        if re.match(r"^(第\s*\d+|Vol\.|Dec\.|202\d年)", line):
            continue
        if any(cue in line for cue in EXCLUDE_CUES):
            continue
        if "摘 要" in line or line == "摘要":
            break
        if re.search(r"(研究|影响|采纳|采用|应用|机制|路径|视角|模型)", line):
            return line
        if idx < 6 and 8 <= len(line) <= 80:
            return line
    return fallback


def guess_authors(text: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    for idx, line in enumerate(lines[:25]):
        if re.search(r"[（(].*?大学|学院|公司", line):
            prev = lines[idx - 1] if idx > 0 else ""
            if 2 <= len(prev) <= 40:
                return prev
    return ""


def guess_method(text: str) -> str:
    patterns = [
        "案例研究",
        "单案例研究",
        "多案例研究",
        "fsQCA",
        "回归分析",
        "元分析",
        "问卷调查",
        "实证研究",
        "事件史分析",
        "准自然实验",
        "文本分析",
        "扎根理论",
        "混合方法",
    ]
    hits = [p for p in patterns if p in text]
    return "、".join(hits[:4])


def guess_theory(text: str) -> str:
    patterns = [
        "TOE",
        "技术—组织—环境",
        "资源编排理论",
        "动态能力",
        "社会技术系统理论",
        "技术接受模型",
        "UTAUT",
        "制度理论",
        "组织学习",
        "可供性",
        "知识基础观",
        "社会学习理论",
        "认知评价",
        "扎根理论",
    ]
    hits = [p for p in patterns if p in text]
    return "、".join(hits[:4])


def split_units(text: str) -> list[str]:
    text = re.sub(r"\n+", "\n", text)
    raw_units = re.split(r"(?<=[。！？；])|\n", text)
    units = []
    for item in raw_units:
        item = item.strip()
        if len(item) < 18:
            continue
        if any(cue in item for cue in EXCLUDE_CUES):
            continue
        if re.search(r"\[\d+(?:[-,]\d+)*\]", item):
            continue
        if re.search(r"［\d+(?:[-,]\d+)*］", item):
            continue
        if any(
            cue in item
            for cue in [
                "已有研究",
                "有学者",
                "文献回顾",
                "综上所述",
                "编者按",
                "本文系",
                "某某学者",
                "研究进展",
            ]
        ):
            continue
        units.append(item)
    return units


def candidate_units(text: str) -> list[dict]:
    units = split_units(text)
    results = []
    seen = set()
    for idx, unit in enumerate(units, start=1):
        if not any(cue in unit for cue in CANDIDATE_CUES):
            continue
        normalized = re.sub(r"\s+", " ", unit)
        if normalized in seen:
            continue
        seen.add(normalized)
        nature = "暂不确定"
        pending = "否"
        if any(x in unit for x in ["驱动", "促进采纳", "影响采纳", "前因", "准备度"]):
            nature = "暂似驱动因素"
        elif any(x in unit for x in ["结果表明", "有助于", "提升", "抑制", "影响企业", "竞争优势"]):
            nature = "暂似结果"
        elif any(x in unit for x in ["调节", "异质性", "条件", "情境"]):
            nature = "暂似条件"
        elif any(x in unit for x in ["机制", "中介", "路径"]):
            nature = "暂似机制"
        if "文献回顾" in unit or "已有研究" in unit:
            pending = "是"
        results.append(
            {
                "source_id": f"S{len(results) + 1}",
                "excerpt": normalized,
                "tentative_nature": nature,
                "pending": pending,
            }
        )
    return results


def build_markdown(record: PaperRecord, candidates: list[dict]) -> str:
    lines = [
        "【文献基本信息】",
        f"- 作者：{record.authors_guess or '待补充'}",
        f"- 年份：{record.year_guess or '待补充'}",
        f"- 题目：{record.title_guess or record.filename}",
        "- 来源：待补充",
        "- 研究对象：待补充",
        f"- 研究方法：{record.method_guess or '待补充'}",
        f"- 理论基础：{record.theory_guess or '待补充'}",
        f"- 与研究主题相关性：{record.relevance}",
        f"- 初步判断：{record.judgment}",
        "",
        "【初级编码】",
    ]
    if not candidates:
        lines.extend(
            [
                "待补充：自动抽取阶段未识别到足够明确的候选编码句，建议人工复核正文中的研究问题、假设、研究发现和结论部分。",
                "",
            ]
        )
    for idx, item in enumerate(candidates, start=1):
        sid = item["source_id"]
        cid = f"C{idx}"
        lines.extend(
            [
                f"{sid} 原文：",
                f"“{item['excerpt']}”",
                "",
                f"{cid} 初级编码：",
                "- 待人工精炼",
                "",
                "暂定性质：",
                f"- {item['tentative_nature']}",
                "",
                "编码理由：",
                "- 待结合全文上下文补写。当前保留该句是因为其疑似涉及研究问题、变量关系、结果、机制或条件表述。",
                "- 自动阶段优先保留原文术语，避免过早抽象。",
                "- 如该句属于引用性综述或主线关联较弱内容，应在人工复核时删除或标记为待议。",
                "",
                "备忘录 Memo：",
                "- 待补写：说明该编码与“企业人工智能技术采用”的关系、与其他编码是否重复，以及是否值得进入下一轮聚焦编码。",
                f"- 是否需要标记为“待议”：{item['pending']}",
                "",
            ]
        )
    lines.extend(
        [
            "【初级编码汇总表】",
            "",
            "| 编码编号 | 原文编号 | 原文摘录 | 初级编码 | 暂定性质 | 是否待议 | 备注 |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for idx, item in enumerate(candidates, start=1):
        excerpt = item["excerpt"].replace("|", " ")
        lines.append(
            f"| C{idx} | {item['source_id']} | {excerpt[:80]} | 待人工精炼 | {item['tentative_nature']} | {item['pending']} | 自动抽取候选句 |"
        )
    lines.extend(
        [
            "",
            "【文献级备忘录】",
            "- 自动阶段说明：本文件已完成文本抽取与候选编码句定位，但仍需人工补全“编码理由”“备忘录”中的细化判断。",
            "- 优先复核摘要、研究问题、假设、结果、结论与未来研究部分，删除纯文献综述、背景铺垫与技术细节。",
        ]
    )
    return "\n".join(lines) + "\n"


def extract_full_text(pdf_path: Path) -> str:
    doc = fitz.open(pdf_path)
    chunks = []
    for page in doc:
        chunks.append(page.get_text())
    return clean_text("\n".join(chunks))


def process_pdf(pdf_path: Path, out_dir: Path) -> PaperRecord:
    full_text = extract_full_text(pdf_path)
    first_pages = "\n".join(full_text.splitlines()[:120])
    relevance, judgment = classify_relevance(pdf_path.stem)
    title_guess = guess_title(first_pages, pdf_path.stem)
    authors_guess = guess_authors(first_pages)
    year_guess = guess_year(first_pages)
    method_guess = guess_method(full_text[:12000])
    theory_guess = guess_theory(full_text[:12000])
    candidates = candidate_units(full_text)

    slug = safe_slug(pdf_path.stem)
    text_dir = out_dir / "texts"
    candidate_dir = out_dir / "candidates"
    coding_dir = out_dir / "coding_drafts"
    text_dir.mkdir(parents=True, exist_ok=True)
    candidate_dir.mkdir(parents=True, exist_ok=True)
    coding_dir.mkdir(parents=True, exist_ok=True)

    (text_dir / f"{slug}.txt").write_text(full_text, encoding="utf-8")
    (candidate_dir / f"{slug}.json").write_text(
        json.dumps(candidates, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    record = PaperRecord(
        filename=pdf_path.name,
        relevance=relevance,
        judgment=judgment,
        page_count=fitz.open(pdf_path).page_count,
        title_guess=title_guess,
        authors_guess=authors_guess,
        year_guess=year_guess,
        method_guess=method_guess,
        theory_guess=theory_guess,
        candidate_count=len(candidates),
    )
    md = build_markdown(record, candidates[:20])
    (coding_dir / f"{slug}.md").write_text(md, encoding="utf-8")
    return record


def main() -> int:
    if len(sys.argv) != 3:
        print("Usage: batch_ai_open_coding_prep.py <pdf_dir> <output_dir>")
        return 1
    pdf_dir = Path(sys.argv[1]).expanduser().resolve()
    out_dir = Path(sys.argv[2]).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(pdf_dir.glob("*.pdf"))
    records = [process_pdf(pdf, out_dir) for pdf in pdfs]

    index_path = out_dir / "index.csv"
    with index_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "filename",
                "title_guess",
                "authors_guess",
                "year_guess",
                "relevance",
                "judgment",
                "page_count",
                "method_guess",
                "theory_guess",
                "candidate_count",
            ]
        )
        for r in records:
            writer.writerow(
                [
                    r.filename,
                    r.title_guess,
                    r.authors_guess,
                    r.year_guess,
                    r.relevance,
                    r.judgment,
                    r.page_count,
                    r.method_guess,
                    r.theory_guess,
                    r.candidate_count,
                ]
            )

    summary = {
        "total_pdfs": len(records),
        "high_relevance": sum(1 for r in records if r.relevance == "高"),
        "medium_relevance": sum(1 for r in records if r.relevance == "中"),
        "low_relevance": sum(1 for r in records if r.relevance == "低"),
    }
    (out_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"index: {index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
