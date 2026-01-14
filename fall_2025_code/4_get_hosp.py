#This file gets the nearest hospital to each author's institution. We need to implement multithreaded calls to the API on 2 cores (the basic one and the fallback one). Also async/aiohttp. Make sure
#To check the rate limits for the two APIs in this case: arcgis open data and nominatin.
import requests
import pandas as pd
import numpy as np
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from sklearn.neighbors import BallTree

# Need to create to all schools
# ── CONFIG ────────────────────────────────────────────────────────────────
INPUT_CSV     = "/home/mm4958/openalex/results/MIT_author_institution_year_spans.csv"
OUTPUT_CSV    = "/home/mm4958/openalex/results/with_nearest_hospital_v4.csv"
HOSPITALS_URL = (
    "https://opendata.arcgis.com/datasets/"
    "f36521f6e07f4a859e838f0ad7536898_0.csv"
)
OA_BASE       = "https://api.openalex.org/institutions/"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
REQUEST_DELAY = 0.1   # seconds between API calls
GEOCODE_DELAY = 1.0   # seconds for Nominatim

# ── SETUP HTTP SESSION WITH RETRIES ─────────────────────────────────────────
session = requests.Session()
retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_json(url, timeout=10):
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def fetch_inst_coord(inst_id):
    """Fetch latitude and longitude for a given OpenAlex institution ID."""
    key = inst_id.rstrip("/").split("/")[-1]
    try:
        js = fetch_json(f"{OA_BASE}{key}")
        geo = js.get("location", {}).get("geo", {})
        lat = geo.get("latitude")
        lon = geo.get("longitude")
        if lat is not None and lon is not None:
            return inst_id, (float(lat), float(lon))
    except Exception:
        pass
    return inst_id, (None, None)

# ── LOAD PANEL DATA ───────────────────────────────────────────────────────
df = pd.read_csv(INPUT_CSV, dtype=str)
unique_insts = df["institution_id"].unique()
id_to_name = df.set_index("institution_id")["institution_name"].to_dict()

# ── PARALLEL GEOCODING via OpenAlex ─────────────────────────────────────────
inst_coords = {}
print(f"Geocoding {len(unique_insts)} institutions via OpenAlex...")
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_inst_coord, iid): iid for iid in unique_insts}
    for i, fut in enumerate(as_completed(futures), start=1):
        inst_id, coord = fut.result()
        inst_coords[inst_id] = coord
        print(f"[Geo {i}/{len(unique_insts)}]")
        time.sleep(REQUEST_DELAY)

# ── FALLBACK GEOCODING via Nominatim for missing coords ──────────────────── #We should parallelize this
print("Fallback geocoding missing institutions via Nominatim...")
for inst_id, (lat, lon) in list(inst_coords.items()):
    if lat is None or lon is None:
        name = id_to_name.get(inst_id, "")
        if name:
            print(f"  [Fallback] Geocoding '{name}'")
            try:
                res = session.get(NOMINATIM_URL, params={
                    "q": name,
                    "format": "json",
                    "limit": 1
                }, headers={"User-Agent":"AcademicHealthPanel/1.0"}).json()
                if res:
                    lat = float(res[0]["lat"])
                    lon = float(res[0]["lon"])
                    inst_coords[inst_id] = (lat, lon)
                    print(f"    → {lat:.4f}, {lon:.4f}")
                else:
                    print("    → No result")
            except Exception as e:
                print(f"    → Error: {e}")
            time.sleep(GEOCODE_DELAY)

# ── LOAD & PREPARE HOSPITAL DATA ───────────────────────────────────────────
print("Downloading hospital list…")
hosp = pd.read_csv(HOSPITALS_URL)
hosp.columns = hosp.columns.str.strip().str.lower()
hosp = hosp.rename(columns={
    "name":      "hospital_name",
    "latitude":  "latitude",
    "longitude": "longitude"
})
hosp = hosp[["hospital_name","latitude","longitude"]].dropna()
hosp["latitude"]  = hosp["latitude"].astype(float)
hosp["longitude"] = hosp["longitude"].astype(float)

# ── BUILD BallTree for fast nearest-hospital lookup ────────────────────────
hosp_rad = np.deg2rad(hosp[["latitude","longitude"]].values)
tree = BallTree(hosp_rad, metric="haversine")

# ── COMPUTE NEAREST HOSPITAL FOR EACH INSTITUTION ─────────────────────────
nearest_map = {}
print("Finding nearest hospital for each institution...")
for i, (inst_id, (lat, lon)) in enumerate(inst_coords.items(), start=1):
    if lat is None or lon is None:
        nearest_map[inst_id] = {"closest_hospital":"","hospital_lat":"","hospital_lon":"","distance_km":""}
    else:
        inst_rad = np.deg2rad([[lat, lon]])
        dist_rad, idx = tree.query(inst_rad, k=1)
        idx0 = idx[0][0]
        d_km = dist_rad[0][0] * 6371.0
        rec = hosp.iloc[idx0]
        nearest_map[inst_id] = {
            "closest_hospital": rec.hospital_name,
            "hospital_lat":     rec.latitude,
            "hospital_lon":     rec.longitude,
            "distance_km":      d_km
        }
    if i % 100 == 0 or i == len(inst_coords):
        print(f"[Hospital {i}/{len(inst_coords)}]")

# ── MERGE & SAVE FINAL PANEL ───────────────────────────────────────────────
print("Merging nearest-hospital info into panel and saving...")
out = df.copy()
out["closest_hospital"] = out["institution_id"].map(lambda x: nearest_map.get(x, {}).get("closest_hospital",""))
out["hospital_lat"]     = out["institution_id"].map(lambda x: nearest_map.get(x, {}).get("hospital_lat",""))
out["hospital_lon"]     = out["institution_id"].map(lambda x: nearest_map.get(x, {}).get("hospital_lon",""))
out["distance_km"]      = out["institution_id"].map(lambda x: nearest_map.get(x, {}).get("distance_km",""))

out.to_csv(OUTPUT_CSV, index=False)
print(f"✓ Done: wrote {len(out)} rows with nearest hospital info to {OUTPUT_CSV}")
