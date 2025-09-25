#This script queries OpenAlex for a list of authors that have been affiliated with a list of
#institutions. It also adds the years they were at the institution.
import requests
import asyncio
import aiohttp
import time
import pandas as pd
from aiohttp import ClientSession, ClientResponseError

# CONFIG: map a short slug to its OpenAlex institution ID
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
YEAR_MIN, YEAR_MAX = 1955, 2025
CHUNK_SIZE = 10  # years per chunk
REQUESTS_PER_SECOND = 1
MAX_REQUESTS = 1000
PER_PAGE = 100
request_times = []
request_count = 0
request_count_lock = asyncio.Lock()

async def rate_limited_fetch(sem, session, url):
    async with sem:
        start = time.time()
        async with session.get(url) as resp:
            resp.raise_for_status()
            result = await resp.json()
        elapsed = time.time() - start
        request_times.append(elapsed)

        # await asyncio.sleep(0.1)
        return result

async def fetch_with_cursor(session, sem, inst_id_num, year_start, year_end):
    cursor = "*"
    all_results = []
    while cursor:

        if len(request_times) >= MAX_REQUESTS:
            print(f"Reached maximum requests limit ({MAX_REQUESTS}). Stopping.")
            break

        url = (
            "https://api.openalex.org/works"
            f"?filter=institutions.id:{inst_id_num},"
            f"publication_year:{year_start}-{year_end}"
            f"&per_page={PER_PAGE}&cursor={cursor}"
        )
        try:
            result = await rate_limited_fetch(sem, session, url)
            all_results.extend(result.get("results", []))
            cursor = result["meta"].get("next_cursor")

            # log progress
            total_requests = len(request_times)
            total_time = sum(request_times)
            avg_rps = total_requests / total_time if total_time > 0 else 0
            print(f"[{year_start}-{year_end}] Total so far: {len(all_results)}, "
                  f"Requests made: {total_requests}, avg {avg_rps:.2f} req/sec")

        except ClientResponseError as e:
            print(f"[{year_start}-{year_end}] Error {e.status}: {e.message}")
            break

    return all_results

async def process_institution(slug, inst_id_num):
    authors = {}
    inst_url = f"https://openalex.org/{inst_id_num}"
    sem = asyncio.Semaphore(REQUESTS_PER_SECOND)  # limit concurrent requests

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        tasks = []
        # Split years into chunks
        for start in range(YEAR_MIN, YEAR_MAX + 1, CHUNK_SIZE):
            end = min(start + CHUNK_SIZE - 1, YEAR_MAX)
            tasks.append(fetch_with_cursor(session, sem, inst_id_num, start, end))

        # Run all chunks in parallel
        all_chunks = await asyncio.gather(*tasks)

    # Flatten results
    all_results = [r for chunk in all_chunks for r in chunk]

    # Process works
    for work in all_results:
        year = work.get("publication_year")
        if not isinstance(year, int) or not (YEAR_MIN <= year <= YEAR_MAX):
            continue

        for auth in work.get("authorships", []):
            a = auth.get("author", {})
            aid = a.get("id")
            name = a.get("display_name", "Unknown")
            if not aid:
                continue

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
                    rec["year_end_1"] = max(rec["year_end_1"], year)

    # dump to CSV
    df = pd.DataFrame.from_dict(authors, orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "author_id"}, inplace=True)
    out_fn = f"/home/mm4958/openalex/results/{slug}_only_affiliations.csv"
    df.to_csv(out_fn, index=False)
    print(f"[{slug}] Saved {len(df)} authors → {out_fn}")

# ── ENTRYPOINT ───────────────────────────────────────────────────────────────
async def main():
    start_time = time.time()

    for slug, props in INSTITUTIONS.items():
        await process_institution(slug, props) #once we add more institutions, this needs to be turned into a task so we can run asynchronously, using semaphore to rate limit to 10 req per second

    elapsed_time = time.time() - start_time
    print(f"\n Finished in {elapsed_time/60:.2f} minutes ({elapsed_time:.2f} seconds)")

if __name__ == "__main__":
    asyncio.run(main())
