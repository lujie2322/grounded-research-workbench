#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path


EXCERPT_CUES = [
    "研究发现",
    "结果表明",
    "得出如下结论",
    "本文发现",
    "本文打开了",
    "有助于",
    "促进",
    "提升",
    "抑制",
    "影响",
    "驱动",
    "通过",
    "机制",
    "中介",
    "调节",
    "异质性",
    "边界条件",
    "未来研究",
    "研究局限",
    "研究展望",
    "提出如下假设",
]

STOP_HEADINGS = ["参考文献", "基金项目", "作者简介"]


def normalize(text: str) -> str:
    text = text.replace("\x00", " ")
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def lines(text: str) -> list[str]:
    return [line.strip() for line in normalize(text).splitlines()]


def read_index(index_path: Path) -> dict[str, dict[str, str]]:
    with index_path.open(encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return {row["filename"]: row for row in rows}


def clean_sentence(text: str) -> str:
    text = text.strip(" ·•-*—:：;；,.，")
    text = re.sub(r"\[[^\]]+\]", "", text)
    text = re.sub(r"［[^］]+］", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def split_sentences(block: str) -> list[str]:
    parts = re.split(r"(?<=[。！？；])", block)
    items = []
    for part in parts:
        s = clean_sentence(part)
        if len(s) >= 16 and not any(stop in s for stop in STOP_HEADINGS):
            items.append(s)
    return items


def first_nonempty(lines_: list[str], start: int, end: int) -> str:
    for line in lines_[start:end]:
        if line:
            return line
    return ""


def extract_authors(lines_: list[str], title: str) -> str:
    for i, line in enumerate(lines_[:20]):
        if title in line:
            candidate = first_nonempty(lines_, i + 1, min(i + 5, len(lines_)))
            if candidate and not any(x in candidate for x in ["摘 要", "摘要", "基金项目", "收稿日期"]):
                return candidate
    for i, line in enumerate(lines_[:10]):
        if re.search(r"[（(].*大学|学院", line):
            prev = lines_[i - 1] if i > 0 else ""
            if 2 <= len(prev) <= 40:
                return prev
    return "待补充"


def extract_source(lines_: list[str]) -> str:
    for line in lines_[:8]:
        if any(x in line for x in ["学报", "管理", "研究", "对策", "论坛", "评论", "季刊"]):
            if "第" in line and "卷" in line:
                continue
            if len(line) <= 30:
                return line
    return "待补充"


def extract_year(text: str) -> str:
    m = re.search(r"(20\d{2})", text)
    return m.group(1) if m else "待补充"


def extract_section(text: str, start_markers: list[str], end_markers: list[str]) -> str:
    start = -1
    for marker in start_markers:
        idx = text.find(marker)
        if idx != -1 and (start == -1 or idx < start):
            start = idx
    if start == -1:
        return ""
    end = len(text)
    for marker in end_markers:
        idx = text.find(marker, start + 1)
        if idx != -1 and idx < end:
            end = idx
    return text[start:end]


def candidate_excerpts(text: str) -> list[str]:
    text = normalize(text)
    blocks = []
    abstract = extract_section(text, ["摘 要", "摘要"], ["关键词", "关键字"])
    if abstract:
        blocks.append(abstract)

    for marker in ["研究问题", "重要问题", "本文拟", "本文聚焦于", "探究", "提出如下假设", "研究假设"]:
        idx = text.find(marker)
        if idx != -1:
            blocks.append(text[max(0, idx - 40): min(len(text), idx + 420)])

    for marker in ["结论与讨论", "结论与启示", "研究结论", "结论", "讨论", "机制分析", "研究局限", "不足与展望", "研究展望", "未来研究"]:
        idx = text.find(marker)
        if idx != -1:
            blocks.append(text[idx: min(len(text), idx + 2200)])

    sentences = []
    seen = set()
    for block in blocks:
        for sentence in split_sentences(block):
            if not any(cue in sentence for cue in EXCERPT_CUES):
                continue
            if "已有研究" in sentence or "文献回顾" in sentence:
                continue
            if re.search(r"\[[0-9,\-]+\]|［[0-9,\-]+］", sentence):
                continue
            if sentence not in seen:
                seen.add(sentence)
                sentences.append(sentence)
    return sentences[:12]


def split_codes(sentence: str) -> list[str]:
    sent = sentence
    sent = sent.replace("研究发现：", "").replace("研究发现:", "")
    sent = sent.replace("结果表明：", "").replace("结果表明:", "")
    sent = sent.replace("得出如下结论：", "").replace("得出如下结论:", "")
    parts = re.split(r"[；;]|、(?=第[一二三四五六七八九十123456789]|[0-9①②③④⑤⑥⑦⑧⑨])|,?并|,?同时|,?通过|,?从而", sent)
    cleaned = []
    for p in parts:
        p = clean_sentence(p)
        if len(p) < 6:
            continue
        p = re.sub(r"^[①②③④⑤⑥⑦⑧⑨0-9]+", "", p).strip()
        if p and p not in cleaned:
            cleaned.append(p)
    return cleaned[:5] or [clean_sentence(sentence)]


def tentative_nature(sentence: str) -> str:
    if any(x in sentence for x in ["驱动", "促进企业AI采纳", "影响AI采纳", "影响企业人工智能应用", "前因", "准备度", "采纳强度受"]):
        return "暂似驱动因素"
    if any(x in sentence for x in ["有助于", "提升", "抑制", "降低", "增加", "影响劳动力需求", "竞争优势", "绩效", "韧性", "创新"]):
        return "暂似结果"
    if any(x in sentence for x in ["调节", "异质性", "边界条件", "情境", "不同条件", "在…情境下", "普适性"]):
        return "暂似条件"
    if any(x in sentence for x in ["机制", "中介", "路径", "通过", "替代效应", "扩张效应"]):
        return "暂似机制"
    return "暂不确定"


def reason_lines(sentence: str, nature: str) -> list[str]:
    lines_ = [
        "- 该句属于作者对研究问题、变量关系、研究发现、机制解释或未来研究方向的直接表述，符合初级编码保留范围。",
        "- 编码尽量保留原文术语，避免在当前阶段过早上升到过于抽象的框架概念。",
    ]
    if nature == "暂似驱动因素":
        lines_.append("- 句中包含对企业AI采纳/应用的推动、影响或前提条件描述，因此暂作驱动性弱判断。")
    elif nature == "暂似结果":
        lines_.append("- 句中出现提升、抑制、影响等结果性措辞，因此暂作结果性弱判断。")
    elif nature == "暂似条件":
        lines_.append("- 句中包含情境差异、调节、边界或普适性限制，因此暂作条件性弱判断。")
    elif nature == "暂似机制":
        lines_.append("- 句中强调通过何种路径、功能或中介作用发挥影响，因此暂作机制性弱判断。")
    else:
        lines_.append("- 该句与研究主线相关，但其更偏研究问题或综合判断，当前不强行归类。")
    return lines_


def memo_lines(sentence: str, relevance: str) -> list[str]:
    lines_ = [
        "- 该条编码与“企业人工智能技术采用”的关系需从企业采纳、实施、嵌入、应用结果或边界条件的角度理解。",
        "- 后续聚焦编码时，可与其他文献中相近表述进行合并或区分，判断其是否形成稳定概念。",
    ]
    if relevance == "高":
        lines_.append("- 文献整体相关性较高，这条编码通常值得优先保留进入下一轮聚焦编码。")
    elif relevance == "中":
        lines_.append("- 文献与主线存在一定关联，建议在下一轮聚焦编码时结合上下文判断保留强度。")
    else:
        lines_.append("- 文献与主线关联相对较弱，建议在下一轮聚焦编码时重点核查其与企业AI采用主线的实际贴合度。")
    if "未来研究" in sentence or "展望" in sentence or "局限" in sentence:
        lines_.append("- 这条内容可作为边界条件或未来研究线索观察。")
    return lines_


def build_doc(meta: dict[str, str], title: str, authors: str, source: str, year: str, excerpts: list[str]) -> str:
    lines_ = [
        "【文献基本信息】",
        f"- 作者：{authors}",
        f"- 年份：{year}",
        f"- 题目：{title}",
        f"- 来源：{source}",
        "- 研究对象：待补充",
        f"- 研究方法：{meta.get('method_guess') or '待补充'}",
        f"- 理论基础：{meta.get('theory_guess') or '待补充'}",
        f"- 与研究主题相关性：{meta.get('relevance', '待补充')}",
        f"- 初步判断：{meta.get('judgment', '待补充')}",
        "",
        "【初级编码】",
    ]

    summary_rows = []
    for idx, excerpt in enumerate(excerpts, start=1):
        sid = f"S{idx}"
        codes = split_codes(excerpt)
        nature = tentative_nature(excerpt)
        lines_.append(f"{sid} 原文：")
        lines_.append(f"“{excerpt}”")
        lines_.append("")
        for code_index, code in enumerate(codes):
            suffix = chr(ord("a") + code_index) if len(codes) > 1 else ""
            cid = f"C{idx}{suffix}"
            lines_.append(f"{cid} 初级编码：")
            lines_.append(f"- {code}")
            lines_.append("")
            lines_.append("暂定性质：")
            lines_.append(f"- {nature}")
            lines_.append("")
            lines_.append("编码理由：")
            lines_.extend(reason_lines(excerpt, nature))
            lines_.append("")
            lines_.append("备忘录 Memo：")
            lines_.extend(memo_lines(excerpt, meta.get("relevance", "")))
            need_pending = "是" if meta.get("relevance") == "低" else "否"
            lines_.append(f"- 是否需要标记为“待议”：{need_pending}")
            lines_.append("")
            summary_rows.append((cid, sid, excerpt[:70], code, nature, need_pending, "自动生成正式编码稿"))

    lines_.append("【初级编码汇总表】")
    lines_.append("")
    lines_.append("| 编码编号 | 原文编号 | 原文摘录 | 初级编码 | 暂定性质 | 是否待议 | 备注 |")
    lines_.append("|---|---|---|---|---|---|---|")
    for row in summary_rows:
        lines_.append(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} |")

    lines_.extend(
        [
            "",
            "【文献级备忘录】",
            "1. 这篇文献最值得保留的初级概念，通常集中在摘要、研究结论、机制分析与研究展望中的高频术语。",
            "2. 与“企业人工智能技术采用”主线最相关的内容，应优先关注企业AI采纳、应用、实施、整合、采纳强度、应用结果及其条件性解释。",
            "3. 下一轮聚焦编码中，应重点观察本篇文献中反复出现的前因词、结果词、机制词和条件词，判断其是否形成稳定范畴。",
            "4. 若文中存在大段理论回顾、政策背景或方法技术细节，应视为辅助信息，不作为核心保留内容。",
            f"5. 基于当前初筛，这篇文献建议判定为“{meta.get('judgment', '待补充')}”，可据后续人工复核进一步调整。",
            "6. 当前文档已保证“原文—初级编码—编码理由—备忘录”链条完整，但仍建议后续结合全文上下文做精修。",
        ]
    )
    return "\n".join(lines_) + "\n"


def main() -> int:
    root = Path("/Users/jie/Desktop/editor/output/ai_open_coding_batch_v2")
    texts_dir = root / "texts"
    final_dir = root / "final_codings_all"
    final_dir.mkdir(parents=True, exist_ok=True)
    index = read_index(root / "index.csv")

    for txt_path in sorted(texts_dir.glob("*.txt")):
        text = txt_path.read_text(encoding="utf-8")
        filename = txt_path.stem + ".pdf"
        meta = index.get(filename, {})
        title = meta.get("filename", filename).removesuffix(".pdf")
        line_list = lines(text)
        authors = extract_authors(line_list, title)
        source = extract_source(line_list)
        year = extract_year(text)
        excerpts = candidate_excerpts(text)
        if not excerpts:
            excerpts = split_sentences(text[:2500])[:6]
        doc = build_doc(meta, title, authors, source, year, excerpts)
        (final_dir / f"{txt_path.stem}.md").write_text(doc, encoding="utf-8")
    print(f"generated: {len(list(final_dir.glob('*.md')))}")
    print(f"output: {final_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
