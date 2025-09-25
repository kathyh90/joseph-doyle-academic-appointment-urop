#fetch 


import pandas as pd
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter
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
    # Add more institutions as needed
}

# ── GLOBALS ──────────────────────────────────────────────────────────────────
author_cache = {}
RATE_LIMIT = 10  # max requests per second

# ── FETCH FUNCTION WITH RATE LIMITER ─────────────────────────────────────────
async def fetch_profile(session, limiter, oid):
    if oid in author_cache:
        return author_cache[oid]

    url = f"https://api.openalex.org/authors/{oid}"
    try:
        async with limiter:
            async with session.get(url) as resp:
                resp.raise_for_status()
                profile = await resp.json()
                author_cache[oid] = profile
                return profile
    except Exception as e:
        author_cache[oid] = {"_error": type(e).__name__}
        return author_cache[oid]

# ── PROFILE PROCESSING ───────────────────────────────────────────────────────
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
            "_error":               profile["_error"]
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

# ── MAIN PROCESSOR PER INSTITUTION ───────────────────────────────────────────
async def process_institution(slug, props, limiter):
    disp_lower = props["display"].lower()
    oa_id      = props["oa_id"]
    in_csv     = props["input"]
    out_csv    = Path("/home/mm4958/openalex/results") / props["output"]
    prefix     = slug.upper()

    if not Path(in_csv).exists():
        print(f"⚠ Skipping {slug}: {in_csv} not found.")
        return

    df = pd.read_csv(in_csv, dtype=str).fillna("")
    total = len(df)
    print(f"\n=== Processing {props['display']} ({total} authors) ===")

    results = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for _, row in df.iterrows():
            oid = row["author_id"].rsplit("/", 1)[-1]
            tasks.append(fetch_profile(session, limiter, oid))

        profiles = await asyncio.gather(*tasks)

        for row, profile in zip(df.to_dict(orient="records"), profiles):
            enriched = {**row, **process_profile(row, profile, oa_id, prefix)}
            results.append(enriched)

    out_df = pd.DataFrame(results)
    out_df["_has_inst"] = out_df["_has_inst"].fillna(False)
    out_df = out_df[out_df["_has_inst"]].drop(columns=["_has_inst", "_error"], errors="ignore")
    out_df.to_csv(out_csv, index=False)
    print(f"✓ Done! Saved {len(out_df)} profiles to {out_csv.name}")

# ── MAIN ENTRYPOINT ──────────────────────────────────────────────────────────
async def main():
    limiter = AsyncLimiter(max_rate=RATE_LIMIT, time_period=1)

    for slug, props in INSTITUTIONS.items():
        await process_institution(slug, props, limiter)

if __name__ == "__main__":
    asyncio.run(main())
