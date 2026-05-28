"""
amazon_scraper.py  ─  Production-Ready Amazon.in Scraper
=========================================================
Features:
  • 30+ product categories (electronics, fashion, home, beauty, etc.)
  • Rotating user-agents + optional proxy support
  • Exponential back-off with jitter on retries
  • Deduplication by ASIN across categories
  • Checkpoint / resume – saves progress after every page
  • Structured logging to file + console
  • Outputs: CSV, JSON, SQLite
  • Rate-limit aware (429 detection + long sleep)
  • CLI flags for targeted runs

Usage:
  python amazon_scraper.py                        # scrape all categories
  python amazon_scraper.py --categories smartphone laptop
  python amazon_scraper.py --pages 5 --threads 2
  python amazon_scraper.py --resume               # continue from checkpoint
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
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─────────────────────────── CONFIG ──────────────────────────────────────────

BASE_URL         = "https://www.amazon.in/"
DEFAULT_PAGES    = 5          # pages per category
DEFAULT_THREADS  = 3          # concurrent threads (keep ≤ 5 to stay safe)
OUTPUT_DIR       = Path("output")
OUTPUT_CSV       = OUTPUT_DIR / "amazon_products.csv"
OUTPUT_JSON      = OUTPUT_DIR / "amazon_products.json"
OUTPUT_DB        = OUTPUT_DIR / "amazon_products.db"
CHECKPOINT_FILE  = OUTPUT_DIR / "checkpoint.json"
LOG_FILE         = OUTPUT_DIR / "scraper.log"

# Full category map  ─  (display_name → search-path fragment)
CATEGORIES: dict[str, str] = {
    # Electronics
    "smartphone":        "s?k=smartphones&i=electronics",
    "laptop":            "s?k=laptops&i=computers",
    "tablet":            "s?k=tablets&i=electronics",
    "smartwatch":        "s?k=smartwatch&i=electronics",
    "headphones":        "s?k=headphones&i=electronics",
    "earbuds":           "s?k=wireless+earbuds&i=electronics",
    "smart_tv":          "s?k=smart+tv&i=electronics",
    "camera":            "s?k=digital+camera&i=electronics",
    "gaming_console":    "s?k=gaming+console&i=videogames",
    "gaming_laptop":     "s?k=gaming+laptop&i=computers",
    "keyboard":          "s?k=mechanical+keyboard&i=computers",
    "mouse":             "s?k=wireless+mouse&i=computers",
    "monitor":           "s?k=computer+monitor&i=computers",
    "hard_drive":        "s?k=external+hard+drive&i=computers",
    "pen_drive":         "s?k=pen+drive&i=computers",
    "router":            "s?k=wifi+router&i=electronics",
    "power_bank":        "s?k=power+bank&i=electronics",
    "phone_charger":     "s?k=fast+charger&i=electronics",
    # Home & Kitchen
    "air_conditioner":   "s?k=air+conditioner&i=kitchen",
    "refrigerator":      "s?k=refrigerator&i=kitchen",
    "washing_machine":   "s?k=washing+machine&i=kitchen",
    "microwave":         "s?k=microwave+oven&i=kitchen",
    "mixer_grinder":     "s?k=mixer+grinder&i=kitchen",
    "air_purifier":      "s?k=air+purifier&i=kitchen",
    "ceiling_fan":       "s?k=ceiling+fan&i=kitchen",
    "water_purifier":    "s?k=water+purifier&i=kitchen",
    # Fashion
    "men_tshirt":        "s?k=men+t-shirt&i=apparel",
    "women_dress":       "s?k=women+dress&i=apparel",
  "men_shoes":         "s?k=men+shoes&i=shoes",
    "women_shoes":       "s?k=women+shoes&i=shoes",
    "backpack":          "s?k=backpack&i=luggage",
    "sunglasses":        "s?k=sunglasses&i=apparel",
    # Beauty & Health
    "face_cream":        "s?k=face+cream&i=beauty",
    "shampoo":           "s?k=shampoo&i=beauty",
    "perfume":           "s?k=perfume&i=beauty",
    "electric_toothbrush":"s?k=electric+toothbrush&i=hpc",
    "fitness_tracker":   "s?k=fitness+tracker&i=electronics",
    # Books & Toys
    "books_bestseller":  "s?k=bestseller+books&i=stripbooks",
    "toys":              "s?k=kids+toys&i=toys",
    # Grocery
    "dry_fruits":        "s?k=dry+fruits&i=grocery",
    "protein_powder":    "s?k=whey+protein+powder&i=grocery",
    # Automotive
    "car_accessories":   "s?k=car+accessories&i=automotive",
    "helmet":            "s?k=bike+helmet&i=automotive",
    # Sports
    "yoga_mat":          "s?k=yoga+mat&i=sports",
    "cricket_bat":       "s?k=cricket+bat&i=sports",
    # Office
    "office_chair":      "s?k=office+chair&i=furniture",
    "desk":              "s?k=computer+desk&i=furniture",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

# ─────────────────────────── LOGGING ─────────────────────────────────────────

OUTPUT_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("amazon_scraper")

# ─────────────────────────── SESSION ─────────────────────────────────────────

session = requests.Session()

# Optional: set proxies here if you have a proxy pool
# session.proxies = {"https": "http://user:pass@host:port"}

ACCEPT_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "DNT":             "1",
}


def _headers() -> dict:
    return {"User-Agent": random.choice(USER_AGENTS), **ACCEPT_HEADERS}


def safe_request(url: str, retries: int = 5) -> str | None:
    """GET with exponential back-off; returns HTML or None."""
    delay = 2.0
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, headers=_headers(), timeout=15)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 429:
                wait = delay * 4 + random.uniform(3, 8)
                log.warning("Rate-limited (429). Sleeping %.1fs …", wait)
                time.sleep(wait)
                delay *= 2
                continue
            if resp.status_code in (503, 403):
                log.warning("Block/unavailable (%s) on attempt %d", resp.status_code, attempt)
            else:
                log.warning("HTTP %s on attempt %d", resp.status_code, attempt)
        except requests.exceptions.RequestException as exc:
            log.warning("Request error attempt %d: %s", attempt, exc)

        sleep_time = delay + random.uniform(0.5, 2.0)
        log.info("Retrying in %.1fs …", sleep_time)
        time.sleep(sleep_time)
        delay = min(delay * 2, 60)

    log.error("All %d retries failed for %s", retries, url)
    return None


# ─────────────────────────── CLEANERS ────────────────────────────────────────

def clean_price(raw: str | None) -> int | None:
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else None


def clean_rating(raw: str | None) -> float | None:
    if not raw:
        return None
    m = re.search(r"([\d.]+)\s*out", raw)
    if m:
        return float(m.group(1))
    m = re.search(r"[\d.]+", raw)
    return float(m.group()) if m else None


def clean_reviews(raw: str | None) -> int | None:
    if not raw:
        return None
    digits = re.sub(r"[^\d]", "", raw)
    return int(digits) if digits else None


# ─────────────────────────── PARSER ──────────────────────────────────────────

def parse_page(html: str, category: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div[data-component-type='s-search-result']")
    results: list[dict] = []

    for card in cards:
        asin = card.get("data-asin", "").strip()
        if not asin:
            continue

        # Title
        title_el = card.select_one("h2 span") or card.select_one("h2 a span")
        title = title_el.get_text(strip=True) if title_el else None
        if not title:
            continue

        # Price (current)
        price_el   = card.select_one(".a-price .a-offscreen")
        price      = clean_price(price_el.get_text() if price_el else None)

        # Original / struck-through price
        orig_els   = card.select(".a-price.a-text-price .a-offscreen")
        orig_price = clean_price(orig_els[0].get_text() if orig_els else None)

        # Discount badge
        badge_el   = card.select_one(".a-badge-text") or card.select_one(".s-color-deal")
        discount   = badge_el.get_text(strip=True) if badge_el else None

        # Rating
        rating_el  = card.select_one(".a-icon-alt")
        rating     = clean_rating(rating_el.get_text() if rating_el else None)

        # Review count – look for the parenthesised number
        review_els = card.select("span[aria-label]")
        reviews    = None
        for el in review_els:
            lbl = el.get("aria-label", "")
            if re.search(r"[\d,]+ rating", lbl):
                reviews = clean_reviews(lbl)
                break
        if reviews is None:
            r_el = card.select_one(".a-size-base.s-underline-text")
            reviews = clean_reviews(r_el.get_text() if r_el else None)

        # Sponsored flag
        sponsored_el = card.select_one(".puis-sponsored-label-text")
        sponsored    = bool(sponsored_el)

        # URL & image
        link_el = card.select_one("h2 a[href]")
        img_el  = card.select_one("img.s-image")

        # Best-seller / Amazon's Choice badge
        choice_el = card.select_one(".a-badge-label")
        badge_lbl = choice_el.get_text(strip=True) if choice_el else None

        results.append({
            "category":     category,
            "asin":         asin,
            "title":        title,
            "price_inr":    price,
            "orig_price":   orig_price,
            "discount":     discount,
            "rating":       rating,
            "reviews":      reviews,
            "badge":        badge_lbl,
            "sponsored":    sponsored,
            "url":          "https://www.amazon.in" + link_el["href"] if link_el else None,
            "img_url":      img_el.get("src") if img_el else None,
            "scraped_at":   datetime.now(timezone.utc).isoformat(),
        })

    return results


# ─────────────────────────── PAGE SCRAPER ────────────────────────────────────

def scrape_page(category: str, path: str, page: int) -> list[dict]:
    url  = f"{BASE_URL}{path}&page={page}"
    log.info("Fetching %-20s  page %2d  →  %s", category, page, url)
    html = safe_request(url)
    if not html:
        log.error("Skipping %s page %d (no HTML)", category, page)
        return []
    products = parse_page(html, category)
    log.info("  ✓ %d products extracted", len(products))
    # polite crawl delay between pages
    time.sleep(random.uniform(1.5, 3.5))
    return products


# ─────────────────────────── CHECKPOINT ──────────────────────────────────────

def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            return json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"scraped_pages": [], "asins": []}


def save_checkpoint(scraped_pages: list, asins: list) -> None:
    CHECKPOINT_FILE.write_text(
        json.dumps({"scraped_pages": scraped_pages, "asins": asins}, indent=2),
        encoding="utf-8",
    )


# ─────────────────────────── SAVE HELPERS ────────────────────────────────────

CSV_FIELDS = [
    "category", "asin", "title", "price_inr", "orig_price", "discount",
    "rating", "reviews", "badge", "sponsored", "url", "img_url", "scraped_at",
]


def save_csv(data: list[dict], path: Path = OUTPUT_CSV) -> None:
    if not data:
        return
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(data)
    log.info("CSV appended  → %s  (%d rows)", path, len(data))


def save_json(data: list[dict], path: Path = OUTPUT_JSON) -> None:
    existing: list[dict] = []
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    existing.extend(data)
    path.write_text(json.dumps(existing, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("JSON saved    → %s  (%d total records)", path, len(existing))


def save_sqlite(data: list[dict], db_path: Path = OUTPUT_DB) -> None:
    if not data:
        return
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            asin        TEXT PRIMARY KEY,
            category    TEXT,
            title       TEXT,
            price_inr   INTEGER,
            orig_price  INTEGER,
            discount    TEXT,
            rating      REAL,
            reviews     INTEGER,
            badge       TEXT,
            sponsored   INTEGER,
            url         TEXT,
            img_url     TEXT,
            scraped_at  TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON products(category)")
    rows = [
        (
            r["asin"], r["category"], r["title"], r["price_inr"], r["orig_price"],
            r["discount"], r["rating"], r["reviews"], r["badge"],
            int(r["sponsored"]), r["url"], r["img_url"], r["scraped_at"],
        )
        for r in data
    ]
    conn.executemany("""
        INSERT OR REPLACE INTO products
        (asin,category,title,price_inr,orig_price,discount,rating,reviews,
         badge,sponsored,url,img_url,scraped_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    conn.close()
    log.info("SQLite saved  → %s  (%d rows upserted)", db_path, len(rows))


# ─────────────────────────── MAIN ORCHESTRATOR ───────────────────────────────

def run(
    categories: dict[str, str],
    pages_per_cat: int = DEFAULT_PAGES,
    max_threads: int   = DEFAULT_THREADS,
    resume: bool       = False,
) -> list[dict]:

    checkpoint = load_checkpoint() if resume else {"scraped_pages": [], "asins": []}
    scraped_pages: list[str] = checkpoint["scraped_pages"]
    seen_asins:    set[str]  = set(checkpoint["asins"])
    all_products:  list[dict] = []

    for cat, path in categories.items():
        log.info("━━━  Category: %s  ━━━", cat.upper())

        tasks = [
            (cat, path, pg)
            for pg in range(1, pages_per_cat + 1)
            if f"{cat}::{pg}" not in scraped_pages
        ]

        if not tasks:
            log.info("  All pages already scraped (resume mode). Skipping.")
            continue

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            future_map = {
                executor.submit(scrape_page, cat, path, pg): (cat, pg)
                for cat, path, pg in tasks
            }
            for future in as_completed(future_map):
                cat_name, pg = future_map[future]
                try:
                    products = future.result()
                except Exception as exc:
                    log.error("Unexpected error for %s page %d: %s", cat_name, pg, exc)
                    products = []

                # Deduplicate
                fresh = [p for p in products if p["asin"] not in seen_asins]
                seen_asins.update(p["asin"] for p in fresh)
                all_products.extend(fresh)

                # Checkpoint after every page
                scraped_pages.append(f"{cat_name}::{pg}")
                save_checkpoint(scraped_pages, list(seen_asins))

                # Incremental saves
                save_csv(fresh)
                save_sqlite(fresh)

        # Polite pause between categories
        time.sleep(random.uniform(3, 6))

    # Final JSON dump (full dataset in one file)
    save_json(all_products)

    log.info("━━━  COMPLETE  ━━━  %d unique products scraped  ━━━", len(all_products))
    return all_products


# ─────────────────────────── CLI ─────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Production Amazon.in scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python amazon_scraper.py\n"
            "  python amazon_scraper.py --categories smartphone laptop\n"
            "  python amazon_scraper.py --pages 10 --threads 4\n"
            "  python amazon_scraper.py --resume\n"
            "  python amazon_scraper.py --list-categories\n"
        ),
    )
    p.add_argument(
        "--categories", nargs="+", metavar="CAT",
        help="Subset of categories to scrape (default: all)",
    )
    p.add_argument(
        "--pages", type=int, default=DEFAULT_PAGES, metavar="N",
        help=f"Pages per category (default: {DEFAULT_PAGES})",
    )
    p.add_argument(
        "--threads", type=int, default=DEFAULT_THREADS, metavar="N",
        help=f"Parallel threads (default: {DEFAULT_THREADS})",
    )
    p.add_argument(
        "--resume", action="store_true",
        help="Resume from last checkpoint",
    )
    p.add_argument(
        "--list-categories", action="store_true",
        help="Print all available category keys and exit",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()

    if args.list_categories:
        print("\nAvailable categories:\n")
        for k in sorted(CATEGORIES):
            print(f"  {k:<25}  →  {CATEGORIES[k]}")
        print()
        return

    selected: dict[str, str] = CATEGORIES
    if args.categories:
        unknown = set(args.categories) - set(CATEGORIES)
        if unknown:
            log.error("Unknown categories: %s", unknown)
            sys.exit(1)
        selected = {k: CATEGORIES[k] for k in args.categories}

    log.info("Starting scraper  |  categories=%d  pages=%d  threads=%d  resume=%s",
             len(selected), args.pages, args.threads, args.resume)

    run(
        categories    = selected,
        pages_per_cat = args.pages,
        max_threads   = args.threads,
        resume        = args.resume,
    )


if __name__ == "__main__":
    main()