
# Final script using Google Maps Routes API v2, change to this if funding is received. 
# It is necessary to add an API key to this code. After this is done, please test for a limited number of data points 
# in order to check the code is working apporpriately, and if that is the code move into running the full dataset. 
# Also, considering that this must run through a long period of time please do so in the cluster. 
# Finally, please change the data paths to your respective files in order to allow the code to run for the correct data, as the current
# paths are the ones I have in my local machine.

import os
import time
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.distance import geodesic
import polyline
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Google Maps API Key ===
GOOGLE_MAPS_API_KEY = "" # <-- paste your key
if not GOOGLE_MAPS_API_KEY or "PASTE-YOUR-KEY" in GOOGLE_MAPS_API_KEY:
    raise RuntimeError("Paste your Google Maps API key into GOOGLE_MAPS_API_KEY before running.")

# === Load Required Data ===
start_time = time.time()
zcta = gpd.read_file(
    r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\nationwide_TIGER\tl_2020_us_zcta520.shp"
).to_crs(epsg=4326)

# Use pandas for CSVs (not geopandas)
od_df = pd.read_csv(
    r"C:\Users\marco\OneDrive\Área de Trabalho\santa_clara_geoids.csv",
    dtype={"h_geocode": str}
)
emissions_df = pd.read_csv(
    r"C:\Users\marco\OneDrive\Área de Trabalho\avg_emissions_per_geoid_SantaClara.csv",
    dtype={"Census Block Group Code": str}
)

# Ensure coordinate columns are numeric (coerce bad rows to NaN, which will be skipped later)
for col in ["home_lat", "home_lon", "work_lat", "work_lon"]:
    od_df[col] = pd.to_numeric(od_df[col], errors="coerce")

print(f"Data loading time: {time.time() - start_time:.2f} seconds")

POLLUTANT_COLS = ['PM25_per_mile', 'SOx_per_mile', 'NOX_per_mile', 'VOC_per_mile', 'NH3_per_mile', 'CO2_per_mile']

# === Google Routes API v2 helper ===
def fetch_route_coords_google(origin, destination):
    """
    origin/destination: (lat, lon)
    Returns list[(lat, lon)] for the route geometry using Google Routes API v2.
    """
    gmaps_url = "https://routes.googleapis.com/directions/v2:computeRoutes"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_MAPS_API_KEY,
        "X-Goog-FieldMask": "routes.polyline.encodedPolyline"
    }
    body = {
        "origin":      {"location": {"latLng": {"latitude": origin[0], "longitude": origin[1]}}},
        "destination": {"location": {"latLng": {"latitude": destination[0], "longitude": destination[1]}}},
        "travelMode": "DRIVE",
        "routingPreference": "TRAFFIC_AWARE"
    }

    backoffs = [0, 1, 2, 4, 8]
    last_err = None
    for delay in backoffs:
        if delay:
            time.sleep(delay)
        resp = requests.post(gmaps_url, headers=headers, json=body, timeout=30)
        if resp.status_code >= 500:
            last_err = RuntimeError(f"Google server error: {resp.status_code}")
            continue
        if resp.status_code != 200:
            # Raise immediately on client-side issues (quota, key restrictions, API not enabled, etc.)
            try:
                msg = resp.json().get("error", {}).get("message", "")
            except Exception:
                msg = resp.text[:300]
            raise RuntimeError(f"Google Maps API error {resp.status_code}: {msg}")

        data = resp.json()
        routes = data.get("routes", [])
        if not routes:
            last_err = RuntimeError("Google returned no routes.")
            continue

        enc = routes[0].get("polyline", {}).get("encodedPolyline")
        if not enc:
            last_err = RuntimeError("No encoded polyline in Google response.")
            continue

        return polyline.decode(enc)

    raise last_err or RuntimeError("Google Directions failed after retries.")

# === Emissions calculation per OD pair ===
def process_route(idx_row):
    idx, row = idx_row

    # Skip if any coords missing
    if pd.isna(row['home_lat']) or pd.isna(row['home_lon']) or pd.isna(row['work_lat']) or pd.isna(row['work_lon']):
        return {'route_idx': idx, 'error': "Missing coordinates"}

    origin = (row['home_lat'], row['home_lon'])
    destination = (row['work_lat'], row['work_lon'])

    # Takes first 12 digits of h_geocode
    origin_tract = str(row['h_geocode'])[:12]

    # Filter emissions_df for a matching GEOID
    emission_row = emissions_df[emissions_df['Census Block Group Code'] == origin_tract]
    if emission_row.empty:
        return {'route_idx': idx, 'error': f"No emissions data for GEOID {origin_tract}"}
    emissions = emission_row.iloc[0]

    try:
        # === GOOGLE ROUTING CALL ===
        route_coords = fetch_route_coords_google(origin, destination)

        # Build segment midpoints + distances
        segments = []
        for a, b in zip(route_coords[:-1], route_coords[1:]):
            distance = geodesic(a, b).miles
            midpoint_geom = Point((a[1] + b[1]) / 2, (a[0] + b[0]) / 2)
            segments.append({'geometry': midpoint_geom, 'distance_miles': distance})

        if not segments:
            return {'route_idx': idx, 'error': "No route segments"}

        seg_df = pd.DataFrame(segments)
        seg_gdf = gpd.GeoDataFrame(seg_df, geometry='geometry', crs='EPSG:4326')

        # More forgiving join (include boundary hits)
        seg_gdf = gpd.sjoin(seg_gdf, zcta[['ZCTA5CE20', 'geometry']], how='left', predicate='intersects')
        seg_gdf = seg_gdf.dropna(subset=['ZCTA5CE20'])

        if seg_gdf.empty:
            return {'route_idx': idx, 'error': "All segment points fell outside ZCTAs after join"}

        # Aggregate total travel miles per ZIP
        zip_dist = seg_gdf.groupby('ZCTA5CE20')['distance_miles'].sum().reset_index()

        # Multiply distance by per-mile emission factors to get emissions on each ZIP
        for pollutant in POLLUTANT_COLS:
            zip_dist[f'{pollutant}_emissions'] = zip_dist['distance_miles'] * emissions[pollutant]

        # Origin/Destination ZIPs
        origin_point = Point(origin[1], origin[0])
        dest_point = Point(destination[1], destination[0])
        origin_hit = zcta[zcta.contains(origin_point)]
        dest_hit = zcta[zcta.contains(dest_point)]
        origin_zip = origin_hit.iloc[0]['ZCTA5CE20'] if not origin_hit.empty else None
        dest_zip = dest_hit.iloc[0]['ZCTA5CE20'] if not dest_hit.empty else None
        if origin_zip is None or dest_zip is None:
            return {'route_idx': idx, 'error': "Origin/Dest ZIP not found"}

        # Build list of per-ZIP emissions for the current route
        emissions_by_zip = []
        for _, row_zip in zip_dist.iterrows():
            emissions_by_zip.append({
                'zip': row_zip['ZCTA5CE20'],
                'PM25': row_zip['PM25_per_mile_emissions'],
                'SOx': row_zip['SOx_per_mile_emissions'],
                'NOX': row_zip['NOX_per_mile_emissions'],
                'VOC': row_zip['VOC_per_mile_emissions'],
                'NH3': row_zip['NH3_per_mile_emissions'],
                'CO2': row_zip['CO2_per_mile_emissions']
            })

        return {
            'route_idx': idx,
            'origin_zip': origin_zip,
            'dest_zip': dest_zip,
            'emissions_by_zip': emissions_by_zip
        }

    except Exception as e:
        return {'route_idx': idx, 'error': str(e)}

# === Run parallel routing with ThreadPool ===
results = []
t_parallel = time.time()
with ThreadPoolExecutor(max_workers=2) as executor:  # keep max_workers=2
    futures = [executor.submit(process_route, item) for item in od_df.head(20).iterrows()]  # <- toggle subset here
    #futures = [executor.submit(process_route, item) for item in od_df.iterrows()]            # <- full dataset
    for future in as_completed(futures):
        results.append(future.result())

print(f"Total execution time: {time.time() - t_parallel:.2f} seconds")

# === Post-processing and Output Aggregation ===
records_A, records_B, records_C = [], [], []

for r in results:
    if 'error' in r or r['origin_zip'] is None or r['dest_zip'] is None:
        continue
    origin_zip = r['origin_zip']
    dest_zip = r['dest_zip']

    for entry in r['emissions_by_zip']:
        receptor_zip = entry['zip']

        # A: Emissions by ZIP as receptor
        records_A.append({'zip': receptor_zip, **entry})

        # B: Origin ZIP causes these emissions
        records_B.append({'origin_zip': origin_zip, 'receptor_zip': receptor_zip, **entry})

        # C: Destination ZIP causes these emissions
        records_C.append({'dest_zip': dest_zip, 'receptor_zip': receptor_zip, **entry})

# === Convert to DataFrames and Aggregate ===
if records_A:
    df_A = pd.DataFrame(records_A).groupby('zip')[['PM25', 'SOx', 'NOX', 'VOC', 'NH3', 'CO2']].sum().reset_index()
else:
    df_A = pd.DataFrame()
if records_B:
    df_B = pd.DataFrame(records_B).groupby('origin_zip')[['PM25', 'SOx', 'NOX', 'VOC', 'NH3', 'CO2']].sum().reset_index()
else:
    df_B = pd.DataFrame()
if records_C:
    df_C = pd.DataFrame(records_C).groupby('dest_zip')[['PM25', 'SOx', 'NOX', 'VOC', 'NH3', 'CO2']].sum().reset_index()
else:
    df_C = pd.DataFrame()
# If you want D later, uncomment the block you already have.

# === Save to CSV with confirmation ===
output_dir = "emissions_outputs"
os.makedirs(output_dir, exist_ok=True)

def save_with_check(df, filename, label):
    path = os.path.join(output_dir, filename)
    if not df.empty:
        df.to_csv(path, index=False)
        print(f"[✓] {label} saved to {path} ({len(df)} rows)")
    else:
        print(f"[!] Warning: {label} DataFrame is empty — no file written.")

save_with_check(df_A, "receptor_zip_emissions_google.csv", "Total emissions per ZIP (receptor)")
save_with_check(df_B, "origin_zip_emissions_google.csv", "Total emissions caused by origin ZIP")
save_with_check(df_C, "destination_zip_emissions_google.csv", "Total emissions caused by destination ZIP")
# save_with_check(df_D, "zip_to_zip_emissions_matrix_google.csv", "ZIP-to-ZIP emissions matrix")