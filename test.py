import pandas as pd
import time
from SPARQLWrapper import SPARQLWrapper, JSON

# ── CONFIG ────────────────────────────────────────────────────────────────
INPUT_CSV         = "/home/kathyh90/joseph-doyle-academic-appointment-urop/results/wikidata_query.csv"
OUTPUT_CSV        = "/home/kathyh90/joseph-doyle-academic-appointment-urop/results/1orcid_wikidata_query.csv"
NAME_COL          = "name"       # name column
ORCID_COL         = "orcid"      # optional column for ORCID IDs
REQUEST_DELAY_SEC = 1.0          # ≤1 req/sec

# ── SETUP ─────────────────────────────────────────────────────────────────
url = "https://query.wikidata.org/sparql"
user_agent = "MITResearchScript/1.0 (mm4958@mit.edu)"
sparql = SPARQLWrapper(url, agent=user_agent)
sparql.setReturnFormat(JSON)

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

# CHANGED ORCID TO ORCID_INPUT

def fetch_dates(label: list, orcid_input: str):
    """
    Try fetching using ORCID first.
    """
    # 1. ORCID query (if available)
    # could add if orcid not in [None, "", "nan", "NaN"]:
    if orcid_input and pd.notna(orcid_input):
        orcid_input = str(orcid_input).strip().upper()
        print(f'orcid: {orcid_input}')
        query = make_orcid_query(orcid=orcid_input)
        sparql.setQuery(query)
        try:
            # results is dictionary
            results = sparql.query().convert()
            # bindings is list of dictionaries
            bindings = results["results"]["bindings"]
            if bindings:
                # list only has one element
                b = bindings[0]
                return (
                    b.get("dob", {}).get("value"),
                    b.get("dod", {}).get("value"),
                    orcid_input,
                    b.get("loc_id", {}).get("value"),
                    b.get("affiliationLabel", {}).get("value"),
                    # would return url, need just the ending qid
                    b.get("person", {}).get("value").split("/")[-1]
                )
        except Exception as e:
            print(f"⚠ ORCID lookup failed for {label[0]} ({orcid_input}): {e}")
    return None, None, None, None, None, None

def main():
    df = pd.read_csv(INPUT_CSV, dtype=str)
    df.columns = df.columns.str.strip()

    if NAME_COL not in df.columns:
        raise KeyError(f"Column '{NAME_COL}' not found in {INPUT_CSV}")
    has_orcid = ORCID_COL in df.columns

    lookup = {}

    unique_names = df[[NAME_COL] + ([ORCID_COL] if has_orcid else [])].drop_duplicates().values.tolist()
    print(f"→ {len(unique_names)} unique researchers to query.")

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

        try_orcid = maybe_orcid[0] if maybe_orcid else None
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

if __name__ == "__main__":
    main()
