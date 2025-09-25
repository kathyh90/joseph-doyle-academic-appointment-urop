#!/usr/bin/env python3
"""
enrich_with_birth_death.py

Reads your `with_nearest_hospital_v4.csv`, looks up each researcher’s
date of birth (P569) and date of death (P570) on Wikidata (filtered to MIT affiliates),
and writes out `with_nearest_hospital_v5.csv` with two new columns:
    - date_of_birth (ISO format)
    - date_of_death (ISO format or blank)

Dependencies:
    pip install pandas SPARQLWrapper
"""

import pandas as pd
import time
from SPARQLWrapper import SPARQLWrapper, JSON


# Need to create to all schools
# ── CONFIG ────────────────────────────────────────────────────────────────
INPUT_CSV         = "/home/mm4958/openalex/results/with_nearest_hospital_v4.csv"
OUTPUT_CSV        = "/home/mm4958/openalex/results/with_nearest_hospital_v5.csv"
NAME_COL          = "name"           # column in your CSV with the researcher’s name
MIT_WIKIDATA_QID  = "Q49117"         # Wikidata Q-ID for MIT
REQUEST_DELAY_SEC = 1.0              # throttle ≤1 request/sec to Wikidata

# ── SET UP SPARQL CLIENT ─────────────────────────────────────────────────
url = "https://query.wikidata.org/sparql"
user_agent = "MITResearchScript/1.0 (mm4958@mit.edu)"
sparql = SPARQLWrapper(url, agent= user_agent)
sparql.setReturnFormat(JSON)

#Not enough dob are being retrieved, consider fuzzy matching
#Add code to get death dates via SSDI
def fetch_dates_from_wikidata(label: str):
    """
    Given an exact English label `label`, returns a tuple
    (date_of_birth, date_of_death) or (None, None) if not found.
    Filters to humans (P31=Q5) affiliated to MIT (P108=Q49117).
    """
    query = f'''
    SELECT ?dob ?dod WHERE {{
      ?person wdt:P31 wd:Q5;                 # instance of human
              rdfs:label "{label}"@en;      # exact English label match
              wdt:P569 ?dob.                # birth date required
      OPTIONAL {{ ?person wdt:P570 ?dod. }} # optional death date
      ?person wdt:P108 wd:{MIT_WIKIDATA_QID}.  affiliation: MIT
    }}
    LIMIT 1
    '''
    sparql.setQuery(query)
    try:
        results = sparql.query().convert()
        bindings = results["results"]["bindings"]
        if not bindings:
            return None, None
        b = bindings[0]
        dob = b["dob"]["value"]
        dod = b.get("dod", {}).get("value")
        return dob, dod
    except Exception as e:
        print(f"⚠ SPARQL lookup error for '{label}': {e}")
        return None, None

def main():
    # 1) Read your existing CSV
    df = pd.read_csv(INPUT_CSV, dtype=str)
    df.columns = df.columns.str.strip()
    if NAME_COL not in df.columns:
        raise KeyError(f"Column '{NAME_COL}' not found in {INPUT_CSV}. Available columns: {df.columns.tolist()}")

    # 2) Get unique names
    unique_names = df[NAME_COL].dropna().unique().tolist()
    print(f"→ {len(unique_names)} unique researcher names to query on Wikidata.")

    # 3) Query Wikidata for each name
    lookup = {}
    for i, name in enumerate(unique_names, start=1):
        print(f"[{i}/{len(unique_names)}] Querying '{name}' …", end="", flush=True)
        dob, dod = fetch_dates_from_wikidata(name)
        lookup[name] = {"date_of_birth": dob, "date_of_death": dod}
        print(f"  → dob={dob!r}, dod={dod!r}")
        time.sleep(REQUEST_DELAY_SEC)

    # 4) Map results back into the DataFrame
    df["date_of_birth"] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("date_of_birth"))
    df["date_of_death" ] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("date_of_death"))

    # 5) Save enriched CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✓ Done! Wrote {len(df)} rows with birth/death dates to '{OUTPUT_CSV}'.")

if __name__ == "__main__":
    main()
