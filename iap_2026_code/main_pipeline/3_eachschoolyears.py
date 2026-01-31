#THis file will get us a timeline of what institutions the author worked at throughout their life.
import asyncio
import aiohttp
import async_timeout
import pandas as pd
from collections import defaultdict
from aiohttp import ClientSession
import time
import math

# ---------------- CONFIG ------------------------
INSTITUTIONS = {
    "mit": {
        "input_profiles_csv": "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/MIT_author_profiles_extended_f.csv",
        "output_spans_csv":   "MIT_author_institution_year_spans.csv"
    },
    "ou": {
        "input_profiles_csv": "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/OU_author_profiles_extended_f.csv",
        "output_spans_csv":   "OU_author_institution_year_spans.csv"
    },
    # "osu": {
    #     "input_profiles_csv": "OSU_author_profiles_extended_f.csv",
    #     "output_spans_csv":   "OSU_author_institution_year_spans.csv"
    # },
    # "dartmouth": {
    #     "input_profiles_csv": "dartmouth_author_profiles_extended_f.csv",
    #     "output_spans_csv":   "dartmouth_author_institution_year_spans.csv"
    # },
    "cornell": {
        "input_profiles_csv": "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/cornell_author_profiles_extended_f.csv",
        "output_spans_csv":   "cornell_author_institution_year_spans.csv"
    },
    # "harvard": {
    #     "input_profiles_csv": "harvard_author_profiles_extended_f.csv",
    #     "output_spans_csv":   "harvard_author_institution_year_spans.csv"
    # },
}

OPENALEX_API_KEY = 'lvSaVMtRlMSlYYbVfXctWl'
PER_PAGE = 200
CONCURRENCY_LIMIT = 5            # how many simultaneous OpenAlex calls
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = 5
REQUEST_DELAY = 0.5               # polite delay between API pages

# ---------------- CACHE -------------------------
# Stores results so the same author is never fetched twice
author_cache = {}   # { author_id : { (inst_id, inst_name): {years} } }


# ---------------- LOW-LEVEL FETCH w/ RETRIES -------------------------
async def fetch_json(url: str, session: ClientSession, semaphore: asyncio.Semaphore):
    HEADERS = {
        "Authorization": f"Bearer {OPENALEX_API_KEY}"
    }

    async with semaphore:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with async_timeout.timeout(30):
                    async with session.get(url, headers = HEADERS) as r:
                        if r.status in RETRY_STATUS_CODES:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        r.raise_for_status()
                        return await r.json()
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"!! Final failure fetching {url}: {e}")
                    return None
                await asyncio.sleep(2 ** attempt)

    return None


# ---------------- FETCH ALL WORKS FOR AN AUTHOR -------------------------
async def fetch_author_works(author_id, session, semaphore):
    """Return a dict: { (inst_id, inst_name) : set(years) } for this author."""
    if author_id in author_cache:
        return author_cache[author_id]

    inst_years = defaultdict(set)
    page = 1

    while True:
        url = (
            "https://api.openalex.org/works"
            f"?filter=authorships.author.id:{author_id}"
            f"&per_page={PER_PAGE}&page={page}"
        )

        data = await fetch_json(url, session, semaphore)
        if not data:
            break

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            yr = work.get("publication_year")
            if not isinstance(yr, int):
                continue

            for auth in work.get("authorships", []):
                author_full_id = auth.get("author", {}).get("id")
                if isinstance(author_full_id, str) and author_full_id.endswith(author_id):
                    for inst in auth.get("institutions", []):
                        inst_id = inst.get("id")
                        inst_name = inst.get("display_name", "")
                        if inst_id:
                            inst_years[(inst_id, inst_name)].add(yr)
                    break

        if len(results) < PER_PAGE:
            break

        await asyncio.sleep(REQUEST_DELAY)
        page += 1

    # store in cache
    author_cache[author_id] = inst_years
    return inst_years


# ---------------- PROCESS ONE INSTITUTION -------------------------
async def process_institution(slug, paths, session, semaphore):
    input_csv  = paths["input_profiles_csv"]
    output_csv = paths["output_spans_csv"]

    try:
        df = pd.read_csv(input_csv, dtype=str)
        # Take 1% sample and overwrite df. DELETE THIS ONCE WE"RE DONE DEBUGGING
        #sample_size = max(1, math.ceil(len(df) * 0.01))
        #df = df.sample(n=sample_size, random_state=42)
    except FileNotFoundError:
        print(f"⚠ Skipping {slug.upper()} — file not found")
        return

    authors = df["author_id"].tolist()
    name_map = dict(zip(df["author_id"], df.get("name", "")))
    orcid_map = dict(zip(df["author_id"], df.get("orcid", "")))

    print(f"\n--- Processing {slug.upper()} ({len(authors)} authors) ---")

    tasks = []
    for aurl in authors:
        author_id = aurl.rstrip("/").split("/")[-1]
        print(f"→ Fetching works for author {author_id}")
        tasks.append(fetch_author_works(author_id, session, semaphore))

    results = await asyncio.gather(*tasks)

    # Assemble all outputs
    rows = []
    for aurl, inst_dict in zip(authors, results):
        author_id = aurl.rstrip("/").split("/")[-1]
        for (inst_id, inst_name), years in inst_dict.items():
            rows.append({
                "author_id":        aurl,
                "name":             name_map.get(aurl, ""),
                "institution_id":   inst_id,
                "institution_name": inst_name,
                "year_start":       min(years),
                "year_end":         max(years),
                "orcid":            orcid_map.get(aurl, "")
            })

    out_df = pd.DataFrame(rows)
    out_path = f"/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/{output_csv}"
    out_df.to_csv(out_path, index=False)
    print(f"✓ Done {slug.upper()} — wrote {len(rows)} rows → {out_df}")


# ---------------- MAIN DRIVER -------------------------
async def main():
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession(connector=connector) as session:
        for slug, paths in INSTITUTIONS.items():
            await process_institution(slug, paths, session, semaphore)


if __name__ == "__main__":
    asyncio.run(main())
