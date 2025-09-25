#This file gets date of death. We should implement async/aiohttp. Make sure to check the rate limit for th ewiki API
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
user_agent = "MITResearchScript/1.0 (mm4958@mit.edu)" #A header for the user agent is required to query
sparql = SPARQLWrapper(url, agent= user_agent) 
sparql.setReturnFormat(JSON)

#Not enough dob are being retrieved, consider fuzzy matching
#Add code to get death dates via SSDI
#Previously, a condition was included in the Sparql query that required the person to be affiliated with MIT. 
#This resulted in a missing dob/dod for most researchers as the Wikibase doesn't include much info on affiliations
#To circumvent this issue, I excluded the MIT condition. I'm not too concerned about this given that we already confirmed affiliations via OpenAlex.
#However, one issue is that we might match to the wrong person. To avoid this, we will also ask for a search to return ORCID, Library of Congress ID, and Affiliations. We will then compare these details to confirm
#it's the right person.
def fetch_dates_from_wikidata(label: str):
    query = f'''
        SELECT ?dob ?dod ?orcid ?loc_id ?affiliationLabel WHERE {{
        SERVICE wikibase:mwapi {{
            bd:serviceParam wikibase:api "EntitySearch";
                            wikibase:endpoint "www.wikidata.org";
                            mwapi:search "{label}";
                            mwapi:language "en".
            ?person wikibase:apiOutputItem mwapi:item.
        }}
        
        ?person wdt:P31 wd:Q5;
                wdt:P569 ?dob.
                
        OPTIONAL {{ ?person wdt:P570 ?dod. }}       # death date
        OPTIONAL {{ ?person wdt:P496 ?orcid. }}     # ORCID
        OPTIONAL {{ ?person wdt:P244 ?loc_id. }}    # Library of Congress ID
        OPTIONAL {{ ?person wdt:P108 ?affiliation. }} # Affiliation(s)
        
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 1
    '''
    sparql.setQuery(query)
    try:
        results = sparql.query().convert()
        bindings = results["results"]["bindings"]
        if not bindings:
            return None, None, None, None, None
        b = bindings[0]
        dob = b["dob"]["value"]
        dod = b.get("dod", {}).get("value")
        orcid = b.get("orcid", {}).get("value")
        loc_id = b.get("loc_id", {}).get("value")
        affiliation = b.get("affiliationLabel", {}).get("value")
        return dob, dod, orcid, loc_id, affiliation
    except Exception as e:
        print(f"⚠ SPARQL lookup error for '{label}': {e}")
        return None, None, None, None, None

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
        dob, dod, orcid, loc_id, affiliation = fetch_dates_from_wikidata(name)
        lookup[name] = {"date_of_birth": dob, "date_of_death": dod, "ORCID": orcid, "loc_id": loc_id, "affiliation": affiliation}
        print(f"  → dob={dob!r}, dod={dod!r}, ORCID={orcid!r}, loc_id={loc_id!r}, affiliation={affiliation!r} ")
        time.sleep(REQUEST_DELAY_SEC)

    # 4) Map results back into the DataFrame
    df["date_of_birth"] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("date_of_birth"))
    df["date_of_death" ] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("date_of_death"))
    df["ORCID" ] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("ORCID"))
    df["loc_id" ] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("loc_id"))
    df["affiliation" ] = df[NAME_COL].map(lambda n: lookup.get(n, {}).get("affiliation"))

    # 5) Save enriched CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✓ Done! Wrote {len(df)} rows with birth/death dates to '{OUTPUT_CSV}'.")

if __name__ == "__main__":
    main()
