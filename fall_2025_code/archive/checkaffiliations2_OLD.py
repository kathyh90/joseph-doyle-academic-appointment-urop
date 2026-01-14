import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ── CONFIG: one entry per institution ─────────────────────────────────────────
INSTITUTIONS = {
    "mit": {
        "display": "massachusetts institute of technology",
        "oa_id":    "https://openalex.org/I63966007",
        "input":    "/home/mm4958/openalex/results/mit_only_affiliations.csv",
        "output":   "/home/mm4958/openalex/results/MIT_author_profiles_extended_f.csv"
    },
    "ou": {
        "display": "university of oklahoma",
        "oa_id":    "https://openalex.org/I8692664",
        "input":    "ou_only_affiliations.csv",
        "output":   "OU_author_profiles_extended_f.csv"
    },
    "osu": {
        "display": "oklahoma state university",
        "oa_id":    "https://openalex.org/I115475287",
        "input":    "osu_only_affiliations.csv",
        "output":   "OSU_author_profiles_extended_f.csv"
    },
    "dartmouth": {
        "display": "dartmouth college",
        "oa_id":    "https://openalex.org/I107672454",
        "input":    "dartmouth_only_affiliations.csv",
        "output":   "DART_author_profiles_extended_f.csv"
    },
    "cornell": {
        "display": "cornell university",
        "oa_id":    "https://openalex.org/I205783295",
        "input":    "cornell_only_affiliations.csv",
        "output":   "CORN_author_profiles_extended_f.csv"
    },
    "harvard": {
        "display": "harvard university",
        "oa_id":    "https://openalex.org/I136199984",
        "input":    "harvard_only_affiliations.csv",
        "output":   "HARV_author_profiles_extended_f.csv"
    }
}

# shared session for HTTP
session = requests.Session()
RATE_LIMIT = 1  # seconds between calls

def fetch_profile_with_retries(oid, tries=2, timeout=10):
    url = f"https://api.openalex.org/authors/{oid}"
    for attempt in range(tries):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(1)

def process_row(idx, row, disp_lower, oa_id, prefix):
    out = dict(row)
    # rename original inst columns
    out[f"{prefix}_ID"]         = out.pop("inst_1_id", None)
    out[f"{prefix}_year_start"] = out.pop("year_start_1", None)
    out[f"{prefix}_year_end"]   = out.pop("year_end_1", None)
    out.pop("inst_1_name", None)

    oid = row["author_id"].rsplit("/", 1)[-1]
    try:
        profile = fetch_profile_with_retries(oid)
    except Exception as e:
        out.update({
            "current_institutions": "",
            "past_institutions":    "",
            "_has_inst":            False,
            "_error":               type(e).__name__
        })
        return idx, out

    current = [inst["display_name"] for inst in profile.get("last_known_institutions", [])]
    past = [
        aff["institution"]["display_name"]
        for aff in profile.get("affiliations", [])
        if aff["institution"]["display_name"] not in current
    ]

    # check for this institution by OA ID
    has_inst = any(inst.get("id", "").lower() == oa_id.lower() for inst in profile.get("last_known_institutions", [])) \
            or any(aff["institution"].get("id", "").lower() == oa_id.lower() for aff in profile.get("affiliations", []))

    out.update({
        "current_institutions": "; ".join(current),
        "past_institutions":    "; ".join(past),
        "_has_inst":            has_inst
    })
    return idx, out

# ── MAIN LOOP ────────────────────────────────────────────────────────────────
for slug, props in INSTITUTIONS.items():
    disp_lower = props["display"].lower()
    oa_id      = props["oa_id"]
    in_csv     = props["input"]
    out_csv    = props["output"]
    prefix     = slug.upper()

    print(f"\n=== Processing {props['display']} ===")
    try:
        df = pd.read_csv(in_csv, dtype=str)
    except FileNotFoundError:
        print(f"⚠ Skipping {slug}: {in_csv} not found.")
        continue

    total = len(df)
    print(f"Starting to process {total} authors for {slug}…")

    executor = ThreadPoolExecutor(max_workers=8)
    futures  = {
        executor.submit(process_row, idx, row, disp_lower, oa_id, prefix): idx
        for idx, row in df.iterrows()
    }

    results = []
    for i, fut in enumerate(as_completed(futures), start=1):
        idx, enriched = fut.result()
        results.append(enriched)
        if i % 50 == 0 or i == total:
            kept = sum(1 for r in results if r.get("_has_inst"))
            print(f"[{i}/{total}] processed — {kept} kept")

    executor.shutdown(wait=True)

    out_df = pd.DataFrame(results)
    out_df["_has_inst"] = out_df["_has_inst"].fillna(False)
    out_df = out_df[out_df["_has_inst"]].drop(columns=["_has_inst", "_error"], errors="ignore")
    out_df.to_csv(out_csv, index=False)
    print(f"✓ Done! Saved {len(out_df)} profiles to {out_csv}")
