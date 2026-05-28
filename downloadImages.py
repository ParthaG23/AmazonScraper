"""
download_images.py  ─  Amazon Product Image Downloader
=======================================================
Reads amazon_products.csv (or JSON / SQLite) produced by amazon_scraper.py
and downloads the FULL / HIGHEST-RESOLUTION version of every product image,
organised into per-category sub-folders.

Resolution upgrade strategy
────────────────────────────
Amazon thumbnail URLs look like:
    https://m.media-amazon.com/images/I/<IMAGE_ID>._AC_UL320_.jpg

By stripping the size modifier we get the original full-size image:
    https://m.media-amazon.com/images/I/<IMAGE_ID>.jpg

Features
─────────
• Full-resolution URL rewriting (strip all size/quality tokens)
• Organises files as  images/<category>/<ASIN>.jpg
• Concurrent downloads with configurable worker pool
• Skips already-downloaded files (idempotent / resumable)
• Retry with exponential back-off per image
• Progress bar via tqdm (falls back gracefully if not installed)
• Saves a download manifest (JSON) with success/failure per ASIN
• CLI flags for source file, output dir, threads, categories

Usage
──────
  python download_images.py                        # from default CSV
  python download_images.py --source output/amazon_products.json
  python download_images.py --categories smartphone laptop
  python download_images.py --threads 8 --output my_images
  python download_images.py --db output/amazon_products.db --categories tablet
"""

import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import urlparse

import requests

# ─────────────────────────── CONFIG ──────────────────────────────────────────

DEFAULT_SOURCE   = Path("output/amazon_products.csv")
DEFAULT_OUT_DIR  = Path("images")
DEFAULT_THREADS  = 8
MANIFEST_FILE    = Path("output/download_manifest.json")
LOG_FILE         = Path("output/image_downloader.log")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.amazon.in/",
    "Accept":  "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}

# ─────────────────────────── LOGGING ─────────────────────────────────────────

Path("output").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("image_downloader")

# ─────────────────────────── PROGRESS BAR ────────────────────────────────────

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

    class tqdm:  # type: ignore[no-redef]
        """Minimal tqdm shim when the real package is absent."""
        def __init__(self, iterable=None, total=0, **kw):
            self._it = iter(iterable) if iterable is not None else iter([])
            self.total = total
            self.n = 0
        def __iter__(self): return self
        def __next__(self):
            item = next(self._it)
            self.n += 1
            pct = int(self.n / self.total * 100) if self.total else 0
            print(f"\r  Progress: {self.n}/{self.total}  ({pct}%)", end="", flush=True)
            return item
        def update(self, n=1): self.n += n
        def close(self): print()
        def __enter__(self): return self
        def __exit__(self, *a): self.close()

# ─────────────────────────── DATA MODEL ──────────────────────────────────────

@dataclass
class ProductRecord:
    asin:     str
    category: str
    img_url:  str | None
    title:    str = ""


@dataclass
class DownloadResult:
    asin:     str
    category: str
    status:   str        # "ok" | "skip" | "error" | "no_url"
    path:     str = ""
    error:    str = ""


# ─────────────────────────── URL REWRITER ────────────────────────────────────

# Amazon size tokens to strip, e.g.  ._AC_UL320_  ._SY300_  ._SX300_QL70_
_SIZE_RE = re.compile(
    r"\._[A-Z0-9_,]+_"      # e.g.  ._AC_UL320_  ._SX300_QL70_FMwebp_
    r"|"
    r"_SX\d+"               # legacy SX/SY
    r"|"
    r"_SY\d+"
    r"|"
    r"\._V\d+_",            # version token  ._V123456789_
    re.IGNORECASE,
)


def to_full_res(url: str | None) -> str | None:
    """
    Strip Amazon thumbnail size tokens to get the full-resolution image URL.

    Input:
        https://m.media-amazon.com/images/I/61fZ9ABCXYZ._AC_UL320_.jpg
    Output:
        https://m.media-amazon.com/images/I/61fZ9ABCXYZ.jpg
    """
    if not url:
        return None

    # Keep only the path up to (and including) the image ID + extension
    # Amazon image IDs look like:  61fZ9ABCXYZ  (alphanumeric)
    #  …/images/I/<ID>.<optional_modifiers>.<ext>
    parsed = urlparse(url)
    path   = parsed.path

    # Strip every modifier group  e.g.  ._AC_SX300_QL70_FMwebp_
    clean_path = _SIZE_RE.sub("", path)

    # Normalise double-dots left over after stripping
    clean_path = re.sub(r"\.{2,}", ".", clean_path)

    return f"https://{parsed.netloc}{clean_path}"


# ─────────────────────────── SESSION ─────────────────────────────────────────

_session = requests.Session()


def download_image(url: str, dest: Path, retries: int = 4) -> bool:
    """Download *url* to *dest*. Returns True on success."""
    delay = 2.0
    for attempt in range(1, retries + 1):
        try:
            resp = _session.get(url, headers=HEADERS, timeout=20, stream=True)
            if resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                # Verify it's actually an image
                if not content_type.startswith("image/"):
                    log.warning("Non-image content-type '%s' for %s", content_type, url)
                    return False
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            if resp.status_code == 429:
                wait = delay * 3 + random.uniform(5, 10)
                log.warning("Rate-limited downloading image; sleeping %.1fs", wait)
                time.sleep(wait)
                delay *= 2
                continue
            log.warning("HTTP %s for %s (attempt %d)", resp.status_code, url, attempt)
        except requests.exceptions.RequestException as exc:
            log.warning("Network error attempt %d for %s: %s", attempt, url, exc)

        time.sleep(delay + random.uniform(0.3, 1.5))
        delay = min(delay * 2, 30)

    return False


# ─────────────────────────── WORKER ──────────────────────────────────────────

def process_record(rec: ProductRecord, out_dir: Path) -> DownloadResult:
    if not rec.img_url:
        return DownloadResult(rec.asin, rec.category, "no_url")

    full_url = to_full_res(rec.img_url)
    if not full_url:
        return DownloadResult(rec.asin, rec.category, "no_url", error="URL rewrite failed")

    # Determine file extension from URL
    ext = Path(urlparse(full_url).path).suffix or ".jpg"
    if ext.lower() not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif"}:
        ext = ".jpg"

    dest = out_dir / rec.category / f"{rec.asin}{ext}"

    if dest.exists() and dest.stat().st_size > 0:
        return DownloadResult(rec.asin, rec.category, "skip", path=str(dest))

    ok = download_image(full_url, dest)
    if ok:
        return DownloadResult(rec.asin, rec.category, "ok", path=str(dest))
    else:
        # Fallback: try the thumbnail URL as-is
        ok2 = download_image(rec.img_url, dest)
        if ok2:
            return DownloadResult(rec.asin, rec.category, "ok", path=str(dest))
        return DownloadResult(
            rec.asin, rec.category, "error",
            error=f"Failed full-res and thumbnail: {full_url}",
        )


# ─────────────────────────── DATA LOADERS ────────────────────────────────────

def load_from_csv(path: Path, categories: set[str] | None = None) -> list[ProductRecord]:
    records: list[ProductRecord] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cat = row.get("category", "").strip()
            if categories and cat not in categories:
                continue
            asin = row.get("asin", "").strip()
            url  = row.get("img_url", "").strip() or None
            if not asin or not url:
                continue
            records.append(ProductRecord(asin, cat, url, row.get("title", "")))
    return records


def load_from_json(path: Path, categories: set[str] | None = None) -> list[ProductRecord]:
    data = json.loads(path.read_text(encoding="utf-8"))
    records: list[ProductRecord] = []
    for row in data:
        cat = row.get("category", "").strip()
        if categories and cat not in categories:
            continue
        asin = row.get("asin", "").strip()
        url  = row.get("img_url", "").strip() or None
        if not asin or not url:
            continue
        records.append(ProductRecord(asin, cat, url, row.get("title", "")))
    return records


def load_from_sqlite(path: Path, categories: set[str] | None = None) -> list[ProductRecord]:
    conn   = sqlite3.connect(path)
    cursor = conn.cursor()
    if categories:
        placeholders = ",".join("?" for _ in categories)
        rows = cursor.execute(
            f"SELECT asin, category, img_url, title FROM products WHERE category IN ({placeholders})",
            list(categories),
        ).fetchall()
    else:
        rows = cursor.execute(
            "SELECT asin, category, img_url, title FROM products"
        ).fetchall()
    conn.close()
    return [
        ProductRecord(asin, cat, url, title or "")
        for asin, cat, url, title in rows
        if asin and url
    ]


def load_records(
    source: Path,
    categories: set[str] | None = None,
) -> list[ProductRecord]:
    ext = source.suffix.lower()
    if ext == ".csv":
        recs = load_from_csv(source, categories)
    elif ext == ".json":
        recs = load_from_json(source, categories)
    elif ext == ".db":
        recs = load_from_sqlite(source, categories)
    else:
        log.error("Unsupported source file type: %s", ext)
        sys.exit(1)

    # Deduplicate by ASIN
    seen: set[str] = set()
    unique: list[ProductRecord] = []
    for r in recs:
        if r.asin not in seen:
            seen.add(r.asin)
            unique.append(r)
    return unique


# ─────────────────────────── MANIFEST ────────────────────────────────────────

def save_manifest(results: list[DownloadResult]) -> None:
    existing: list[dict] = []
    if MANIFEST_FILE.exists():
        try:
            existing = json.loads(MANIFEST_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    # Merge/update by ASIN
    merged = {r["asin"]: r for r in existing}
    for r in results:
        merged[r.asin] = asdict(r)
    MANIFEST_FILE.write_text(
        json.dumps(list(merged.values()), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("Manifest saved → %s  (%d total records)", MANIFEST_FILE, len(merged))


# ─────────────────────────── MAIN ORCHESTRATOR ───────────────────────────────

def run(
    source:     Path,
    out_dir:    Path,
    threads:    int,
    categories: set[str] | None = None,
) -> None:

    log.info("Loading records from %s …", source)
    records = load_records(source, categories)
    if not records:
        log.warning("No records found. Nothing to download.")
        return

    log.info("Found %d unique ASINs across %d categories",
             len(records), len({r.category for r in records}))

    results: list[DownloadResult] = []
    counts   = {"ok": 0, "skip": 0, "error": 0, "no_url": 0}

    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_map = {
            executor.submit(process_record, rec, out_dir): rec
            for rec in records
        }
        with tqdm(total=len(records), desc="Downloading", unit="img") as bar:
            for future in as_completed(future_map):
                try:
                    result = future.result()
                except Exception as exc:
                    rec = future_map[future]
                    result = DownloadResult(rec.asin, rec.category, "error", error=str(exc))

                results.append(result)
                counts[result.status] += 1
                bar.update(1)

    save_manifest(results)

    log.info(
        "━━━ Download complete ━━━  "
        "✓ new=%-5d  skip=%-5d  ✗ error=%-5d  no_url=%d",
        counts["ok"], counts["skip"], counts["error"], counts["no_url"],
    )
    log.info("Images saved under:  %s/", out_dir)


# ─────────────────────────── CLI ─────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download full-resolution Amazon product images by category",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python download_images.py\n"
            "  python download_images.py --source output/amazon_products.json\n"
            "  python download_images.py --db output/amazon_products.db --categories smartphone\n"
            "  python download_images.py --categories laptop tablet --threads 10\n"
            "  python download_images.py --output my_images --threads 12\n"
        ),
    )
    p.add_argument(
        "--source", type=Path, default=DEFAULT_SOURCE,
        help=f"CSV or JSON source file (default: {DEFAULT_SOURCE})",
    )
    p.add_argument(
        "--db", type=Path, default=None,
        help="Use a SQLite .db file instead of CSV/JSON",
    )
    p.add_argument(
        "--output", type=Path, default=DEFAULT_OUT_DIR,
        help=f"Root output directory for images (default: {DEFAULT_OUT_DIR})",
    )
    p.add_argument(
        "--threads", type=int, default=DEFAULT_THREADS,
        help=f"Concurrent download threads (default: {DEFAULT_THREADS})",
    )
    p.add_argument(
        "--categories", nargs="+", metavar="CAT",
        help="Download only these categories (default: all)",
    )
    return p


def main() -> None:
    args   = build_parser().parse_args()
    source = args.db if args.db else args.source

    if not source.exists():
        log.error("Source file not found: %s", source)
        log.error("Run amazon_scraper.py first to generate product data.")
        sys.exit(1)

    cats = set(args.categories) if args.categories else None

    log.info(
        "Image downloader started  |  source=%s  output=%s  threads=%d  categories=%s",
        source, args.output, args.threads,
        sorted(cats) if cats else "ALL",
    )

    run(
        source     = source,
        out_dir    = args.output,
        threads    = args.threads,
        categories = cats,
    )


if __name__ == "__main__":
    main()