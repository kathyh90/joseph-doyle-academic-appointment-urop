#This file gets the institutional lifespan of each author. We need to implement async/aiohttp instead of multithreading. Also author caching.

import requests
import time
import pandas as pd
from collections import defaultdict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ── CONFIG: one entry per institution ─────────────────────────────────────────
INSTITUTIONS = {
    "mit": {
        "input_profiles_csv": "/home/mm4958/openalex/results/MIT_author_profiles_extended_f.csv",
        "output_spans_csv":   "MIT_author_institution_year_spans.csv"
    },
    "ou": {
        "input_profiles_csv": "OU_author_profiles_extended_f.csv",
        "output_spans_csv":   "OU_author_institution_year_spans.csv"
    },
    "osu": {
        "input_profiles_csv": "OSU_author_profiles_extended_f.csv",
        "output_spans_csv":   "OSU_author_institution_year_spans.csv"
    },
    "dartmouth": {
        "input_profiles_csv": "DART_author_profiles_extended_f.csv",
        "output_spans_csv":   "DART_author_institution_year_spans.csv"
    },
    "cornell": {
        "input_profiles_csv": "CORN_author_profiles_extended_f.csv",
        "output_spans_csv":   "CORN_author_institution_year_spans.csv"
    },
    "harvard": {
        "input_profiles_csv": "HARV_author_profiles_extended_f.csv",
        "output_spans_csv":   "HARV_author_institution_year_spans.csv"
    },
}

# how many works per page (max 200)
PER_PAGE      = 200
# polite pause between pages (seconds)
REQUEST_DELAY = 0.5

# ── SETUP SESSION WITH RETRIES ───────────────────────────────────────────────
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def get_json_with_retries(url, timeout=10):
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# ── MAIN LOOP: generate institution-year spans per institution ──────────────
for slug, paths in INSTITUTIONS.items():
    input_csv  = paths["input_profiles_csv"]
    output_csv = paths["output_spans_csv"]

    try:
        authors_df = pd.read_csv(input_csv, dtype=str)
    except FileNotFoundError:
        print(f"⚠ Skipping {slug.upper()}: {input_csv} not found.")
        continue

    name_map    = dict(zip(authors_df["author_id"], authors_df.get("name", "")))
    author_urls = authors_df["author_id"].tolist()
    total = len(author_urls)
    print(f"\n--- Processing {slug.upper()} ({total} authors) ---")

    inst_years = defaultdict(set)

    for author_url in author_urls:
        author_id = author_url.rstrip("/").split("/")[-1]
        print(f"→ Fetching works for author {author_id}")
        page = 1

        while True:
            works_url = (
                "https://api.openalex.org/works"
                f"?filter=authorships.author.id:{author_id}"
                f"&per_page={PER_PAGE}&page={page}"
            )
            try:
                data = get_json_with_retries(works_url)
            except Exception as e:
                print(f"   ! Failed to fetch page {page} for {author_id}: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            for work in results:
                year = work.get("publication_year")
                if not isinstance(year, int):
                    continue

                for auth in work.get("authorships", []):
                    if auth.get("author", {}).get("id", "").endswith(author_id):
                        for inst in auth.get("institutions", []):
                            inst_id   = inst.get("id")
                            inst_name = inst.get("display_name", "")
                            if inst_id:
                                inst_years[(author_url, inst_id, inst_name)].add(year)
                        break  # proceed to next work

            if len(results) < PER_PAGE:
                break

            page += 1
            time.sleep(REQUEST_DELAY)

    # Build and save the spans DataFrame
    records = []
    for (author_url, inst_id, inst_name), years in inst_years.items():
        records.append({
            "author_id":        author_url,
            "name":             name_map.get(author_url, ""),
            "institution_id":   inst_id,
            "institution_name": inst_name,
            "year_start":       min(years),
            "year_end":         max(years),
        })

    out_df = pd.DataFrame(records)
    out_df.to_csv(f"/home/mm4958/openalex/results/{output_csv}", index=False)
    print(f"✓ Done for {slug.upper()}: wrote {len(out_df)} rows to {output_csv}")
