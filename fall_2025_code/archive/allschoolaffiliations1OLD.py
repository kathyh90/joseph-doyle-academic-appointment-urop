import requests
import asyncio
import aiohttp
import time
import pandas as pd

request_times = []
# ── CONFIG: map a short slug to its OpenAlex institution ID ────────────────
INSTITUTIONS = {
    "mit":      "I63966007"   # Massachusetts Institute of Technology
    #"ou":       "I8692664",    # University of Oklahoma
    #"osu":      "I115475287",   # Oklahoma State University
    #"dartmouth":"I107672454",  # Dartmouth College
    #"cornell":  "I205783295",  # Cornell University
    #"harvard":  "I136199984",  # Harvard University
}
HEADERS = {
    "User-Agent": "MyResearchScraper/1.0 (mailto:mm4958@mit.edu)"
}
PER_PAGE  = 100
MAX_PAGES = 500
YEAR_MIN, YEAR_MAX = 1955, 2025
REQUESTS_PER_SECOND = 1
semaphore = asyncio.Semaphore(REQUESTS_PER_SECOND)
DELAY = 1.5    # polite pause between pages

for slug, inst_id_num in INSTITUTIONS.items():
    inst_url = f"https://openalex.org/{inst_id_num}"
    authors = {}

    print(f"\n=== Collecting for {slug.upper()} ({inst_url}) ===")

    for page in range(1, MAX_PAGES+1):
        
        start_time = time.time()
        print(f"[{slug}] Fetching page {page}…")
        url = (
            "https://api.openalex.org/works"
            f"?filter=institutions.id:{inst_id_num},publication_year:{YEAR_MIN}-{YEAR_MAX}"
            f"&per_page={PER_PAGE}&page={page}"
        )
        resp = requests.get(url, headers=HEADERS)
        end_time = time.time()
        elapsed = end_time - start_time
        request_times.append(elapsed)
        
        if resp.status_code != 200:
            print(f"[{slug}] Error on page {page}: {resp.status_code}")
            break

        data = resp.json().get("results", [])
        if not data:
            print(f"[{slug}] No more results.")
            break

        for work in data:
            year = work.get("publication_year")
            if not isinstance(year, int) or not (YEAR_MIN <= year <= YEAR_MAX):
                continue

            for auth in work.get("authorships", []):
                a = auth.get("author", {})
                aid = a.get("id"); name = a.get("display_name", "Unknown")
                if not aid:
                    continue

                # check if this work lists our target inst
                for inst in auth.get("institutions", []):
                    iid = inst.get("id")
                    iname = inst.get("display_name", "Unknown")
                    if iid and iid.lower() == inst_url.lower():
                        rec = authors.setdefault(aid, {
                            "name": name,
                            "inst_1_id": iid,
                            "inst_1_name": iname,
                            "year_start_1": year,
                            "year_end_1": year
                        })
                        rec["year_start_1"] = min(rec["year_start_1"], year)
                        rec["year_end_1"]   = max(rec["year_end_1"],   year)
                        break
                    
        if page % 10 == 0:
            total_time = sum(request_times)
            avg_rps = len(request_times) / total_time
            print(f"Made {len(request_times)} requests in {total_time:.2f}s → avg {avg_rps:.2f} req/sec") 

        time.sleep(DELAY)

    # dump to CSV
    df = pd.DataFrame.from_dict(authors, orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index":"author_id"}, inplace=True)
    out_fn = f"/home/mm4958/openalex/results/{slug}_only_affiliations.csv"
    df.to_csv(out_fn, index=False)
    print(f"[{slug}] Saved {len(df)} authors → {out_fn}")
