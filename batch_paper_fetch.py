#!/usr/bin/env python3
import argparse
import csv
import html
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


USER_AGENT = "paper-fetcher/1.0 (mailto:openalex@example.com)"


def slugify(text: str, limit: int = 100) -> str:
    text = re.sub(r"[^\w\s-]", "", text, flags=re.UNICODE)
    text = re.sub(r"[\s_-]+", "-", text.strip(), flags=re.UNICODE)
    return text[:limit].strip("-") or "paper"


def load_titles(path: Path) -> list[str]:
    titles = []
    seen = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        title = raw.strip()
        if not title or title in seen:
            continue
        seen.add(title)
        titles.append(title)
    return titles


def load_completed_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def get_json(url: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_url(url: str, timeout: int = 30) -> tuple[str, str, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return (
            resp.geturl(),
            (resp.headers.get("Content-Type", "") or "").lower(),
            resp.read(),
        )


def normalize_title(text: str) -> str:
    text = text.casefold()
    text = re.sub(r"\s+", " ", text)
    return re.sub(r"[^\w\s]", "", text).strip()


def title_score(query: str, candidate: str) -> float:
    q = normalize_title(query)
    c = normalize_title(candidate)
    if q == c:
        return 1.0
    if q in c or c in q:
        shorter = min(len(q), len(c)) or 1
        longer = max(len(q), len(c)) or 1
        return shorter / longer
    q_words = set(q.split())
    c_words = set(c.split())
    if not q_words or not c_words:
        return 0.0
    return len(q_words & c_words) / len(q_words | c_words)


def search_openalex(title: str) -> dict | None:
    params = urllib.parse.urlencode(
        {"search": title, "per-page": 5, "mailto": "openalex@example.com"}
    )
    data = get_json(f"https://api.openalex.org/works?{params}")
    best = None
    best_score = 0.0
    for item in data.get("results", []):
        candidate = item.get("display_name") or ""
        score = title_score(title, candidate)
        if score > best_score:
            best = item
            best_score = score
    if not best or best_score < 0.72:
        return None
    oa = best.get("best_oa_location") or {}
    pdf_url = oa.get("pdf_url")
    landing_url = oa.get("landing_page_url")
    doi = (best.get("doi") or "").replace("https://doi.org/", "")
    return {
        "source": "OpenAlex",
        "match_score": round(best_score, 3),
        "matched_title": best.get("display_name") or "",
        "doi": doi,
        "year": best.get("publication_year") or "",
        "pdf_url": pdf_url or "",
        "landing_url": landing_url or "",
    }


def search_crossref(title: str) -> dict | None:
    params = urllib.parse.urlencode({"query.title": title, "rows": 5})
    data = get_json(f"https://api.crossref.org/works?{params}")
    best = None
    best_score = 0.0
    for item in data.get("message", {}).get("items", []):
        candidate = (item.get("title") or [""])[0]
        score = title_score(title, candidate)
        if score > best_score:
            best = item
            best_score = score
    if not best or best_score < 0.72:
        return None
    doi = best.get("DOI", "")
    date_parts = (
        best.get("published-print", {}).get("date-parts", [[None]])
        or best.get("published-online", {}).get("date-parts", [[None]])
    )
    return {
        "source": "Crossref",
        "match_score": round(best_score, 3),
        "matched_title": (best.get("title") or [""])[0],
        "doi": doi,
        "year": date_parts[0][0] or "",
        "pdf_url": "",
        "landing_url": f"https://doi.org/{doi}" if doi else "",
    }


def try_download(url: str, dest: Path, timeout: int = 60) -> tuple[bool, str]:
    if not url:
        return False, "no url"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content_type = resp.headers.get("Content-Type", "").lower()
            final_url = resp.geturl()
            data = resp.read()
    except urllib.error.HTTPError as e:
        return False, f"http {e.code}"
    except urllib.error.URLError as e:
        return False, f"url error: {e.reason}"
    except Exception as e:
        return False, str(e)

    is_pdf = (
        data.startswith(b"%PDF")
        or "application/pdf" in content_type
        or final_url.lower().endswith(".pdf")
    )
    if not is_pdf:
        return False, f"not pdf ({content_type or final_url})"
    dest.write_bytes(data)
    return True, "downloaded"


def discover_pdf_url(landing_url: str) -> tuple[str, str]:
    if not landing_url:
        return "", "no landing url"
    try:
        final_url, content_type, data = fetch_url(landing_url, timeout=60)
    except urllib.error.HTTPError as e:
        return "", f"http {e.code}"
    except urllib.error.URLError as e:
        return "", f"url error: {e.reason}"
    except Exception as e:
        return "", str(e)

    if "application/pdf" in content_type or final_url.lower().endswith(".pdf") or data.startswith(b"%PDF"):
        return final_url, "landing page resolved directly to pdf"

    html_text = data.decode("utf-8", "ignore")
    patterns = [
        r'<meta[^>]+name=["\']citation_pdf_url["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']citation_pdf_url["\']',
        r'href=["\']([^"\']+\.pdf(?:\?[^"\']*)?)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, re.IGNORECASE)
        if match:
            return urllib.parse.urljoin(final_url, html.unescape(match.group(1))), "pdf link found on landing page"

    return "", "no pdf link found on landing page"


def process_title(title: str, pdf_dir: Path) -> dict:
    result = {
        "query_title": title,
        "status": "not_found",
        "source": "",
        "match_score": "",
        "matched_title": "",
        "year": "",
        "doi": "",
        "pdf_url": "",
        "landing_url": "",
        "local_file": "",
        "note": "",
    }

    try:
        meta = search_openalex(title)
        if not meta:
            meta = search_crossref(title)
        if not meta:
            result["note"] = "no confident metadata match"
            return result

        result.update(meta)
        pdf_url = meta.get("pdf_url", "")
        if not pdf_url:
            landing_url = meta.get("landing_url", "")
            if not landing_url and meta.get("doi"):
                landing_url = f"https://doi.org/{meta['doi']}"
                result["landing_url"] = landing_url
            if landing_url:
                pdf_url, discovery_note = discover_pdf_url(landing_url)
                if pdf_url:
                    result["pdf_url"] = pdf_url
                else:
                    result["note"] = discovery_note
        if pdf_url:
            filename = f"{slugify(title)}.pdf"
            dest = pdf_dir / filename
            ok, note = try_download(pdf_url, dest)
            if ok:
                result["status"] = "downloaded"
                result["local_file"] = str(dest)
                result["note"] = note
                return result
            result["note"] = note

        result["status"] = "metadata_only"
        if not result["note"]:
            result["note"] = "matched record but no open-access pdf url"
        return result
    except Exception as e:
        result["status"] = "error"
        result["note"] = str(e)
        return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Batch fetch open-access papers from a title list."
    )
    parser.add_argument("titles_file", type=Path, help="UTF-8 text file, one title per line")
    parser.add_argument("--outdir", type=Path, default=Path("./paper_fetch_output"))
    parser.add_argument("--delay", type=float, default=0.4, help="Delay between requests")
    parser.add_argument(
        "--retry-metadata",
        action="store_true",
        help="Retry rows previously marked as metadata_only using landing-page PDF discovery",
    )
    args = parser.parse_args()

    titles = load_titles(args.titles_file)
    outdir = args.outdir.resolve()
    pdf_dir = outdir / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    results_csv = outdir / "results.csv"

    rows = load_completed_rows(results_csv)
    rows_by_title = {row["query_title"]: row for row in rows}
    completed_titles = set()
    for row in rows:
        status = row.get("status")
        if status == "downloaded" or status == "not_found":
            completed_titles.add(row["query_title"])
        elif status == "metadata_only" and not args.retry_metadata:
            completed_titles.add(row["query_title"])
    pending_titles = [title for title in titles if title not in completed_titles]
    total = len(titles)
    fieldnames = [
        "query_title",
        "status",
        "source",
        "match_score",
        "matched_title",
        "year",
        "doi",
        "pdf_url",
        "landing_url",
        "local_file",
        "note",
    ]
    if not results_csv.exists():
        with results_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    for title in pending_titles:
        idx = len(rows) + 1
        print(f"[{idx}/{total}] {title}", file=sys.stderr)
        row = process_title(title, pdf_dir)
        rows_by_title[title] = row
        rows = [rows_by_title[key] for key in titles if key in rows_by_title]
        with results_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        time.sleep(args.delay)

    summary = {
        "titles": total,
        "downloaded": sum(1 for row in rows if row["status"] == "downloaded"),
        "metadata_only": sum(1 for row in rows if row["status"] == "metadata_only"),
        "not_found": sum(1 for row in rows if row["status"] == "not_found"),
        "errors": sum(1 for row in rows if row["status"] == "error"),
        "results_csv": str(results_csv),
        "pdf_dir": str(pdf_dir),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
