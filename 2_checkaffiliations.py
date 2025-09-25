import pandas as pd
import aiohttp
import asyncio
import time
from pathlib import Path

# ── CONFIG: Institutions ─────────────────────────────────────────────────────
INSTITUTIONS = {
    "mit": {
        "display": "massachusetts institute of technology",
        "oa_id":    "https://openalex.org/I63966007",
        "input":    "/home/mm4958/openalex/results/mit_only_affiliations.csv",
        "output":   "MIT_author_profiles_extended_f.csv"
    },
    "ou": {
        "display": "university of oklahoma",
        "oa_id":    "https://openalex.org/I8692664",
        "input":    "ou_only_affiliations.csv",
        "output":   "OU_author_profiles_extended_f.csv"
    },
    # Add more institutions as needed...
}

# ── GLOBALS ──────────────────────────────────────────────────────────────────
RATE_LIMIT = 10  # requests per second
author_cache = {}  # Cache results by OpenAlex ID
MAX_LINES = 100000  # stop after this many authors

# ── UTILITY ──────────────────────────────────────────────────────────────────
async def rate_limited_fetch(sem, session, url, max_retries=5, base_delay=1.0):
    async with sem:
        for attempt in range(1, max_retries + 1):
            try:
                await asyncio.sleep(10 / RATE_LIMIT)  # space requests
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientResponseError as e:
                if e.status == 429 and attempt < max_retries:
                    delay = base_delay * (2 ** (attempt - 1))  # exponential backoff
                    print(f"429 rate limit hit, retrying in {delay:.1f}s (attempt {attempt}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                # for other HTTP errors or last retry, re-raise
                raise

async def fetch_profile(session, sem, oid):
    if oid in author_cache:
        return author_cache[oid]

    url = f"https://api.openalex.org/authors/{oid}"
    try:
        profile = await rate_limited_fetch(sem, session, url)
        author_cache[oid] = profile
        return profile
    except aiohttp.ClientResponseError as e:
        # store HTTP status code and message
        author_cache[oid] = {
            "_error": type(e).__name__,
            "status": e.status,
            "message": str(e),
            "author_id": oid
        }
        return author_cache[oid]
    except Exception as e:
        # for all other exceptions, put None for status
        author_cache[oid] = {
            "_error": type(e).__name__,
            "status": None,
            "message": str(e),
            "author_id": oid
        }
        return author_cache[oid]
    
async def fetch_and_process(session, sem, row, oa_id, prefix):
    oid = row["author_id"].rsplit("/", 1)[-1]
    profile = await fetch_profile(session, sem, oid)
    enriched = {**row, **process_profile(row, profile, oa_id, prefix)}
    return enriched

def process_profile(row, profile, oa_id, prefix):
    out = {}

    out[f"{prefix}_ID"]         = row.get("inst_1_id")
    out[f"{prefix}_year_start"] = row.get("year_start_1")
    out[f"{prefix}_year_end"]   = row.get("year_end_1")

    if "_error" in profile:
        out.update({
            "current_institutions": "",
            "past_institutions":    "",
            "_has_inst":            False,
            "_error":               profile.get("_error"),
            "status":               profile["status"],
            "message":              profile["message"]
        })
        return out

    current = [inst["display_name"] for inst in profile.get("last_known_institutions", [])]
    past = [
        aff["institution"]["display_name"]
        for aff in profile.get("affiliations", [])
        if aff["institution"]["display_name"] not in current
    ]

    orcid = profile.get("orcid") or profile.get("ids", {}).get("orcid")
    orcid = orcid.rsplit("/", 1)[-1] if orcid else None

    has_inst = any(inst.get("id", "").lower() == oa_id.lower()
                   for inst in profile.get("last_known_institutions", [])) or \
               any(aff["institution"].get("id", "").lower() == oa_id.lower()
                   for aff in profile.get("affiliations", []))

    out.update({
        "current_institutions": "; ".join(current),
        "past_institutions":    "; ".join(past),
        "_has_inst":            has_inst,
        "orcid":                orcid
    })
    return out

# ── MAIN ASYNC RUNNER ────────────────────────────────────────────────────────
async def process_institution(slug, props):
    disp_lower = props["display"].lower()
    oa_id      = props["oa_id"]
    in_csv     = props["input"]
    out_csv    = Path("/home/mm4958/openalex/results") / props["output"]
    prefix     = slug.upper()

    if not Path(in_csv).exists():
        print(f"⚠ Skipping {slug}: {in_csv} not found.")
        return

    df = pd.read_csv(in_csv, dtype=str)
    df = df.fillna("")
    total = len(df)
    print(f"\n=== Processing {props['display']} ({total} authors) ===")

    sem = asyncio.Semaphore(RATE_LIMIT)  # 10 req/sec limiter
    results = []
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_and_process(session, sem, row, oa_id, prefix)
            for _, row in df.iterrows()
        ]

        results = []
        
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            enriched = await task
            results.append(enriched)
            
            if i >= MAX_LINES:  # stop early
                print(f"Reached {MAX_LINES} rows, stopping early.")
                break

            if i % 50 == 0 or i == total:
                kept = sum(1 for r in results if r.get("_has_inst"))
                errors = sum(1 for r in results if r.get("_error"))
                print(f"[{i}/{total}] processed — {kept} kept, {errors} errors")
            
        

    errors = [r for r in results if r.get("_error")]
    print(errors)
    print(f"Total errors: {len(errors)}")

    from collections import Counter
    error_types = Counter(r["_error"] for r in errors)
    print("Error types:", error_types)
    #Save errors to a CSV
    if errors:
    # ensure all columns are included
        all_keys = set().union(*[r.keys() for r in errors])
        pd.DataFrame(errors)[list(all_keys)].to_csv(f"{prefix}_errors.csv", index=False)

    out_df = pd.DataFrame(results)
    out_df["_has_inst"] = out_df["_has_inst"].fillna(False)
    out_df = out_df[out_df["_has_inst"]].drop(columns=["_has_inst", "_error"], errors="ignore")
    out_df.to_csv(out_csv, index=False)
    print(f"✓ Done! Saved {len(out_df)} profiles to {out_csv.name}")

# ── ENTRYPOINT ───────────────────────────────────────────────────────────────
async def main():
    start_time = time.time()
    for slug, props in INSTITUTIONS.items():
        await process_institution(slug, props)
        
    elapsed_time = time.time() - start_time
    print(f"\n Finished in {elapsed_time/60:.2f} minutes ({elapsed_time:.2f} seconds)")

if __name__ == "__main__":
    asyncio.run(main())
