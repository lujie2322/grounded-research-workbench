from __future__ import annotations

import json
import math
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import fitz
import pandas as pd

try:
    from docx import Document  # type: ignore
except Exception:  # pragma: no cover
    Document = None


DESKTOP_ROOT = Path.home() / "Desktop"

PAPER_CODING_SUFFIXES = {".pdf", ".docx", ".txt", ".md"}
META_ANALYSIS_SUFFIXES = {".pdf", ".docx", ".txt", ".md", ".csv", ".xlsx", ".xls"}
INTERVIEW_SUFFIXES = {".docx", ".txt", ".md", ".pdf"}


@dataclass
class SourceFile:
    path: str
    name: str
    suffix: str
    size_bytes: int
    estimated_pages: int
    source_root: str
    source_kind: str


@dataclass
class BatchInfo:
    batch_id: str
    file_count: int
    total_size_mb: float
    total_estimated_pages: int
    file_names: list[str]
    file_paths: list[str]


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_desktop_directories(limit: int = 80) -> list[Path]:
    if not DESKTOP_ROOT.exists():
        return []
    candidates = [path for path in DESKTOP_ROOT.iterdir() if path.is_dir() and not path.name.startswith(".")]
    return sorted(candidates, key=lambda item: item.name.lower())[:limit]


def normalize_input_paths(raw_paths: list[str]) -> list[Path]:
    paths: list[Path] = []
    seen: set[str] = set()
    for raw in raw_paths:
        if not raw.strip():
            continue
        candidate = Path(raw.strip()).expanduser()
        if not candidate.exists():
            continue
        key = str(candidate.resolve())
        if key in seen:
            continue
        seen.add(key)
        paths.append(candidate.resolve())
    return paths


def estimate_pages(path: Path) -> int:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        try:
            with fitz.open(path) as doc:
                return max(doc.page_count, 1)
        except Exception:
            return max(math.ceil(path.stat().st_size / 180_000), 1)
    if suffix == ".docx" and Document is not None:
        try:
            doc = Document(path)
            chars = sum(len(paragraph.text) for paragraph in doc.paragraphs)
            return max(math.ceil(chars / 1800), 1)
        except Exception:
            pass
    try:
        chars = len(path.read_text(encoding="utf-8", errors="ignore"))
    except Exception:
        chars = max(int(path.stat().st_size / 2), 0)
    return max(math.ceil(chars / 1800), 1)


def scan_source_files(
    input_paths: list[Path],
    *,
    allowed_suffixes: set[str],
    recursive: bool = True,
) -> list[SourceFile]:
    files: list[SourceFile] = []
    seen: set[str] = set()
    for source_path in input_paths:
        candidates = list(source_path.rglob("*")) if source_path.is_dir() and recursive else [source_path]
        for candidate in candidates:
            if not candidate.is_file():
                continue
            if candidate.name.startswith(".~") or candidate.name.startswith("~$"):
                continue
            suffix = candidate.suffix.lower()
            if suffix not in allowed_suffixes:
                continue
            resolved = str(candidate.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            stat = candidate.stat()
            files.append(
                SourceFile(
                    path=resolved,
                    name=candidate.name,
                    suffix=suffix,
                    size_bytes=stat.st_size,
                    estimated_pages=estimate_pages(candidate),
                    source_root=str(source_path),
                    source_kind="directory" if source_path.is_dir() else "file",
                )
            )
    return sorted(files, key=lambda item: (item.suffix, item.name.lower()))


def split_into_batches(
    files: list[SourceFile],
    *,
    max_files_per_batch: int = 25,
    max_pages_per_batch: int = 500,
    max_size_mb_per_batch: int = 120,
) -> list[BatchInfo]:
    batches: list[BatchInfo] = []
    current: list[SourceFile] = []
    current_pages = 0
    current_size = 0
    size_limit_bytes = max_size_mb_per_batch * 1024 * 1024

    def flush() -> None:
        nonlocal current, current_pages, current_size
        if not current:
            return
        index = len(batches) + 1
        batches.append(
            BatchInfo(
                batch_id=f"batch_{index:03d}",
                file_count=len(current),
                total_size_mb=round(current_size / (1024 * 1024), 2),
                total_estimated_pages=current_pages,
                file_names=[item.name for item in current],
                file_paths=[item.path for item in current],
            )
        )
        current = []
        current_pages = 0
        current_size = 0

    for item in sorted(files, key=lambda row: (row.estimated_pages, row.size_bytes), reverse=True):
        exceeds_count = current and len(current) >= max_files_per_batch
        exceeds_pages = current and current_pages + item.estimated_pages > max_pages_per_batch
        exceeds_size = current and current_size + item.size_bytes > size_limit_bytes
        if exceeds_count or exceeds_pages or exceeds_size:
            flush()
        current.append(item)
        current_pages += item.estimated_pages
        current_size += item.size_bytes
    flush()
    return batches


def write_inventory(run_dir: Path, files: list[SourceFile], batches: list[BatchInfo]) -> dict[str, str]:
    inventory_dir = ensure_dir(run_dir / "batch_inventory")
    inventory_csv = inventory_dir / "source_inventory.csv"
    batch_csv = inventory_dir / "batch_summary.csv"
    batch_json = inventory_dir / "batch_manifest.json"

    file_to_batch: dict[str, str] = {}
    for batch in batches:
        for path in batch.file_paths:
            file_to_batch[path] = batch.batch_id

    file_rows = []
    for item in files:
        row = asdict(item)
        row["batch_id"] = file_to_batch.get(item.path, "")
        row["size_mb"] = round(item.size_bytes / (1024 * 1024), 2)
        file_rows.append(row)
    pd.DataFrame(file_rows).to_csv(inventory_csv, index=False)
    pd.DataFrame([asdict(batch) for batch in batches]).to_csv(batch_csv, index=False)
    batch_json.write_text(
        json.dumps(
            {
                "total_files": len(files),
                "total_batches": len(batches),
                "batches": [asdict(batch) for batch in batches],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "inventory_csv": str(inventory_csv),
        "batch_csv": str(batch_csv),
        "batch_json": str(batch_json),
    }


def build_batch_symlink_folders(run_dir: Path, batches: list[BatchInfo]) -> list[str]:
    batch_root = ensure_dir(run_dir / "batch_inputs")
    batch_dirs: list[str] = []
    for batch in batches:
        batch_dir = ensure_dir(batch_root / batch.batch_id)
        for file_path in batch.file_paths:
            source = Path(file_path)
            target = batch_dir / source.name
            if target.exists():
                continue
            try:
                target.symlink_to(source)
            except Exception:
                shutil.copy2(source, target)
        batch_dirs.append(str(batch_dir))
    return batch_dirs


def extract_text_from_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        try:
            with fitz.open(path) as doc:
                return "\n".join(doc.load_page(i).get_text() for i in range(min(doc.page_count, 80)))
        except Exception:
            return ""
    if suffix == ".docx" and Document is not None:
        try:
            doc = Document(path)
            return "\n".join(paragraph.text for paragraph in doc.paragraphs if paragraph.text.strip())
        except Exception:
            return ""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def split_text_segments(text: str, *, chunk_chars: int = 2200) -> list[str]:
    text = text.replace("\x00", " ").strip()
    if not text:
        return []
    paragraphs = [part.strip() for part in text.splitlines() if part.strip()]
    if not paragraphs:
        return []
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = f"{current}\n{paragraph}".strip()
        if current and len(candidate) > chunk_chars:
            chunks.append(current.strip())
            current = paragraph
        else:
            current = candidate
    if current.strip():
        chunks.append(current.strip())
    return chunks


def build_meta_analysis_template(run_dir: Path, files: list[SourceFile], batches: list[BatchInfo]) -> str:
    batch_lookup = {}
    for batch in batches:
        for file_path in batch.file_paths:
            batch_lookup[file_path] = batch.batch_id
    rows = []
    for item in files:
        rows.append(
            {
                "batch_id": batch_lookup.get(item.path, ""),
                "filename": item.name,
                "path": item.path,
                "title_guess": Path(item.name).stem,
                "year": "",
                "country": "",
                "sample_size": "",
                "industry": "",
                "independent_var": "",
                "dependent_var": "",
                "mediator": "",
                "moderator": "",
                "effect_size": "",
                "effect_direction": "",
                "method": "",
                "notes": "",
            }
        )
    output = run_dir / "meta_analysis_extraction_template.csv"
    pd.DataFrame(rows).to_csv(output, index=False)
    return str(output)


def build_interview_segments(run_dir: Path, files: list[SourceFile], *, chunk_chars: int = 2200) -> dict[str, str]:
    segment_rows: list[dict[str, Any]] = []
    packet_dir = ensure_dir(run_dir / "coding_packets")
    for item in files:
        path = Path(item.path)
        text = extract_text_from_path(path)
        chunks = split_text_segments(text, chunk_chars=chunk_chars)
        if not chunks:
            continue
        packet_path = packet_dir / f"{path.stem[:80]}.md"
        lines = [f"# {path.name}", ""]
        for index, chunk in enumerate(chunks, start=1):
            segment_id = f"{path.stem}_seg_{index:03d}"
            segment_rows.append(
                {
                    "filename": path.name,
                    "path": str(path),
                    "segment_id": segment_id,
                    "segment_index": index,
                    "text": chunk,
                }
            )
            lines.extend([f"## {segment_id}", "", chunk, ""])
        packet_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    segment_jsonl = run_dir / "interview_segments.jsonl"
    with segment_jsonl.open("w", encoding="utf-8") as handle:
        for row in segment_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    segment_csv = run_dir / "interview_segments.csv"
    pd.DataFrame(segment_rows).to_csv(segment_csv, index=False)
    return {
        "segment_jsonl": str(segment_jsonl),
        "segment_csv": str(segment_csv),
        "packet_dir": str(packet_dir),
    }
