# TO DO: implement parallel querying through asyncio and aiohttp. I started inserting some code
# from other files in the corresponding locations but never ran it through and tested it.
"""
enrich_with_birth_death.py

Reads your CSV, looks up each researcher’s date of birth (P569) and date of death (P570)
on Wikidata — using ORCID (if available) or name fallback — and writes out an enriched CSV.

Dependencies:
    pip install pandas SPARQLWrapper
"""

import pandas as pd
import asyncio
import aiohttp
import time
from SPARQLWrapper import SPARQLWrapper, JSON

# ── CONFIG ────────────────────────────────────────────────────────────────
INSTITUTIONS = {
    "mit": {
        "input":  "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/removed_mit_author.csv",
        "output": "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/with_death_dates_mit.csv"
    },
    "cornell": {
        "input":  "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/with_nearest_hospital_cornell.csv",
        "output": "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/with_death_dates_cornell.csv"
    },
    "OU": {
        "input":  "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/with_nearest_hospital_ou.csv",
        "output": "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/with_death_dates_ou.csv"
    },
},
INPUT_CSV = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/with_nearest_hospital_ou.csv"
OUTPUT_CSV = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/with_death_dates_ou.csv"
NAME_COL          = "name"       # name column
ORCID_COL         = "orcid"      # optional column for ORCID IDs
REQUEST_DELAY_SEC = 1.0          # ≤1 req/sec

# ── SETUP ─────────────────────────────────────────────────────────────────
url = "https://query.wikidata.org/sparql"
user_agent = "MITResearchScript/1.0 (mm4958@mit.edu)"
sparql = SPARQLWrapper(url, agent=user_agent)
sparql.setReturnFormat(JSON)

# ── QUERY HELPERS ─────────────────────────────────────────────────────────
def make_orcid_query(orcid):
    """Return a SPARQL query string for ORCID."""
    return f"""
    SELECT ?person ?dob ?dod ?loc_id ?affiliationLabel WHERE {{
        ?person wdt:P496 "{orcid}" .

        OPTIONAL {{ ?person wdt:P569 ?dob. }}
        OPTIONAL {{ ?person wdt:P570 ?dod. }}
        OPTIONAL {{ ?person wdt:P244 ?loc_id. }}
        OPTIONAL {{ ?person wdt:P108 ?affiliation. }}

        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 1
    """

def make_name_query(name=None):
    """Return a SPARQL query string for name."""
    return f"""
    SELECT ?person ?dob ?dod ?orcid ?loc_id ?affiliationLabel WHERE {{
        SERVICE wikibase:mwapi {{
            bd:serviceParam wikibase:api "EntitySearch";
                            wikibase:endpoint "www.wikidata.org";
                            mwapi:search "{name}";
                            mwapi:language "en".
            ?person wikibase:apiOutputItem mwapi:item .
        }}
        ?person wdt:P31 wd:Q5 .
        OPTIONAL {{ ?person wdt:P569 ?dob. }}
        OPTIONAL {{ ?person wdt:P570 ?dod. }}
        OPTIONAL {{ ?person wdt:P496 ?orcid. }}
        OPTIONAL {{ ?person wdt:P244 ?loc_id. }}
        OPTIONAL {{ ?person wdt:P108 ?affiliation. }}
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    LIMIT 1
    """


def fetch_dates(label: list, orcid: str | None = None):
    """
    Try fetching using ORCID first (if provided), then fall back to name with combinations
    of first + middle + last or just first + last.
    """
    # 1. ORCID query (if available)
    # no need for None check?
    if orcid:
        print(f'orcid: {orcid}')
        query = make_orcid_query(orcid=orcid)
        sparql.setQuery(query)
        try:
            results = sparql.query().convert()
            bindings = results["results"]["bindings"]
            if bindings:
                b = bindings[0]
                return (
                    b.get("dob", {}).get("value"),
                    b.get("dod", {}).get("value"),
                    # CHANGED
                    #b.get("orcid", {}).get("value"),
                    orcid,
                    b.get("loc_id", {}).get("value"),
                    b.get("affiliationLabel", {}).get("value"),
                    # would return url, need just the ending qid
                    b.get("person", {}).get("value").split("/")[-1]
                )
        except Exception as e:
            print(f"⚠ ORCID lookup failed for {label[0]} ({orcid}): {e}")

    # 2. Fallback: name-based lookup
    for each_name in label:
        print(f'each name: {each_name}')
        query = make_name_query(name=each_name)
        sparql.setQuery(query)
        try:
            results = sparql.query().convert()
            bindings = results["results"]["bindings"]

            if not bindings:
                #return None, None, None, None, None, None
                print(f"No match for name variant: {each_name}")
                continue

            b = bindings[0]
            return (
                b.get("dob", {}).get("value"),
                b.get("dod", {}).get("value"),
                b.get("orcid", {}).get("value"),
                b.get("loc_id", {}).get("value"),
                b.get("affiliationLabel", {}).get("value"),
                b.get("person", {}).get("value").split("/")[-1]
            )
        except Exception as e:
            print(f"⚠ Name lookup failed for {label}: {e}")

    # added one more column for qid
    return None, None, None, None, None, None

# ── MAIN ──────────────────────────────────────────────────────────────────
def main():
    df = pd.read_csv(INPUT_CSV, dtype=str)
    df.columns = df.columns.str.strip()

    if NAME_COL not in df.columns:
        raise KeyError(f"Column '{NAME_COL}' not found in {INPUT_CSV}")
    has_orcid = ORCID_COL in df.columns

    unique_names = df[[NAME_COL] + ([ORCID_COL] if has_orcid else [])].drop_duplicates().values.tolist()
    print(f"→ {len(unique_names)} unique researchers to query.")

    lookup = {}
    for i, (name, *maybe_orcid) in enumerate(unique_names, start=1):
        parts = name.split()

        # only first name
        if len(parts) == 1:
            first = parts[0]
            middle = ""
            last = ""
        # only first and last name
        elif len(parts) == 2:
            first, last = parts
            middle = ""
        # all three
        else:
            first = parts[0]
            last = parts[-1]
            # in case multiple middle names
            middle = " ".join(parts[1:-1])

        # Create name combinations
        names_try = []
        # First + Middle + Last
        if middle:
            names_try.append(f"{first} {middle} {last}")
        # First + Last
        if last:
            names_try.append(f"{first} {last}")

        # Always include the original name
        if name not in names_try:
            names_try.append(name)

        print(f'this is names_try: {names_try}')

        print(f'maybe orcid: {maybe_orcid}')
        # clean orcid input if exists
        try_orcid = maybe_orcid[0].strip() if pd.notna(maybe_orcid[0]) else None

        # try_orcid = try_orcid.upper()

        print(f"[{i}/{len(unique_names)}] Querying '{name}' (ORCID={try_orcid}) …", end="", flush=True)
        dob, dod, fetched_orcid, loc_id, affiliation, qid = fetch_dates(names_try, try_orcid)
        lookup[name] = {
            "date_of_birth": dob,
            "date_of_death": dod,
            "ORCID": fetched_orcid,
            "loc_id": loc_id,
            "affiliation": affiliation,
            "q_id": qid
        }
        print(f" → dob={dob!r}, dod={dod!r}, affil={affiliation!r}")
        time.sleep(REQUEST_DELAY_SEC)

    # Merge back into df
    df["date_of_birth"] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("date_of_birth"))
    df["date_of_death"] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("date_of_death"))
    df["ORCID"]         = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("ORCID"))
    df["loc_id"]        = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("loc_id"))
    df["affiliation"]   = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("affiliation"))
    df["q_id"]          = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("q_id"))

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✓ Done! Wrote {len(df)} rows to '{OUTPUT_CSV}'.")

# async def run_all():
#     for slug, props in INSTITUTIONS.items():
#         print(f"\n=== Processing {slug.upper()} ===")
#         await main(slug, props)

if __name__ == "__main__":
    # asyncio.run(run_all())
    main()
