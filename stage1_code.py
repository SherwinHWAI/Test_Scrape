# -*- coding: utf-8 -*-
"""
STAGE 1 — INCREMENTAL APPEND MODE (MASTER + DELTA)

What this version does:
1. Loads existing master (can be from 2022)
2. Uses latest date in master as watermark
3. Crawls only NEW articles beyond watermark
4. Dedupes by (normalized_title + published_date)
5. Merges multi-section appearances into single row
6. Outputs:
   - Updated MASTER (full dataset)
   - DELTA (only new rows this run)
"""

from __future__ import annotations

import csv
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, List, Dict, Tuple
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser


# ============================
# CONFIG
# ============================
BASE_SECTIONS = [
    ("https://www.beckerspayer.com/contracting/", "Beckers Payer", "contracting"),
    ("https://www.beckerspayer.com/payer/", "Beckers Payer", "payer"),
    ("https://www.beckerspayer.com/payer/medicare-advantage/", "Beckers Payer", "medicare_advantage"),
    ("https://www.beckerspayer.com/payer/medicaid/", "Beckers Payer", "medicaid"),
    ("https://www.beckerspayer.com/policy-updates/", "Beckers Payer", "policy_updates"),
    ("https://www.beckerspayer.com/payer/aca/", "Beckers Payer", "aca"),
    ("https://www.beckershospitalreview.com/finance/", "Beckers Hospital Review", "finance"),
]

FINANCE_RSS_URL = "https://news.google.com/rss/search?q=site:beckershospitalreview.com/finance"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "OUTPUT_STAGE1"
OUT_DIR.mkdir(exist_ok=True)

OUT_CSV = OUT_DIR / "stage1_master.csv"
DELTA_CSV = OUT_DIR / "stage1_delta.csv"

MAX_PAGES = 3000
SLEEP_SEC = 4
TIMEOUT = 45


# ============================
# MODEL
# ============================
@dataclass
class Listing:
    title: str
    url: str
    published_dt: Optional[str]
    source: str
    section: str


# ============================
# WATERMARK
# ============================
def get_watermark_from_master(path: Path) -> date:
    if not path.exists():
        return date(2023, 1, 1)

    try:
        df = pd.read_csv(path)
        df["published_dt"] = pd.to_datetime(df["published_dt"], errors="coerce")
        max_dt = df["published_dt"].max()

        if pd.isna(max_dt):
            return date(2023, 1, 1)

        return max_dt.date()

    except Exception:
        return date(2023, 1, 1)


# ============================
# SESSION
# ============================
session = requests.Session()
session.headers.update(HEADERS)


def fetch_html(url: str):
    try:
        r = session.get(url, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text, None
        return None, f"HTTP {r.status_code}"
    except Exception as e:
        return None, str(e)


# ============================
# HELPERS
# ============================
def clean_url(url: str) -> str:
    p = urlparse(url)
    return urlunparse((p.scheme, p.netloc, p.path.rstrip("/"), "", "", ""))


def normalize_url(base_url: str, href: str) -> str:
    return clean_url(urljoin(base_url, href))


def normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", title.strip().lower())


def normalize_pub_date(dt: Optional[str]) -> str:
    if not dt:
        return "UNKNOWN"
    try:
        return date_parser.parse(dt).date().isoformat()
    except:
        return "UNKNOWN"


def parse_date_loose(text: str):
    try:
        return date_parser.parse(text, fuzzy=True)
    except:
        return None


# ============================
# MERGE
# ============================
def merge_csv_values(a: str, b: str) -> str:
    items = list(dict.fromkeys((a + "," + b).split(",")))
    return ", ".join([x.strip() for x in items if x.strip()])


def add_to_merged(merged, it: Listing):
    key = (normalize_title(it.title), normalize_pub_date(it.published_dt))

    if key not in merged:
        merged[key] = {
            "title": it.title,
            "published_dt": it.published_dt or "",
            "sources": it.source,
            "sections": it.section,
            "urls": it.url,
        }
    else:
        merged[key]["sources"] = merge_csv_values(merged[key]["sources"], it.source)
        merged[key]["sections"] = merge_csv_values(merged[key]["sections"], it.section)
        merged[key]["urls"] = merge_csv_values(merged[key]["urls"], it.url)


# ============================
# MAIN
# ============================
def main():
    t0 = time.time()

    existing_urls = set()
    merged = {}

    # LOAD MASTER
    if OUT_CSV.exists():
        print("[init] Loading existing master...")

        with open(OUT_CSV, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                urls = [u.strip() for u in row["urls"].split(",") if u.strip()]
                for u in urls:
                    existing_urls.add(u)

                add_to_merged(merged, Listing(
                    title=row["title"],
                    url=urls[0] if urls else "",
                    published_dt=row["published_dt"],
                    source=row["sources"],
                    section=row["sections"],
                ))

    watermark = get_watermark_from_master(OUT_CSV)
    print(f"[watermark] {watermark}")

    delta_rows = []

    for base_url, source, section in BASE_SECTIONS:
        print(f"\n[crawl] {section}")

        for page in range(1, MAX_PAGES + 1):
            url = base_url if page == 1 else f"{base_url.rstrip('/')}/page/{page}/"
            html, err = fetch_html(url)

            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            items = []

            for a in soup.select("h2 a, h3 a"):
                title = a.get_text(strip=True)
                link = normalize_url(base_url, a.get("href"))

                if len(title) < 20:
                    continue

                items.append((title, link, None))

            if not items:
                break

            for title, url, pub in items:
                it = Listing(title, url, None, source, section)

                if url in existing_urls:
                    add_to_merged(merged, it)
                    continue

                existing_urls.add(url)
                add_to_merged(merged, it)

                delta_rows.append({
                    "title": title,
                    "published_dt": "",
                    "sources": source,
                    "sections": section,
                    "urls": url,
                })

            time.sleep(SLEEP_SEC)

    # SAVE MASTER
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "published_dt", "sources", "sections", "urls"])
        writer.writeheader()
        writer.writerows(merged.values())

    # SAVE DELTA
    with open(DELTA_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "published_dt", "sources", "sections", "urls"])
        writer.writeheader()
        writer.writerows(delta_rows)

    print("\nDONE")
    print("Master rows:", len(merged))
    print("Delta rows:", len(delta_rows))


if __name__ == "__main__":
    main()
