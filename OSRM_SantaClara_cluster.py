#This is the final model file, which will take the necessary 

# This is the final script, made to run on the great lakes cluster using the OSRM method. It will take previous 
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from geopy.distance import geodesic
import polyline
import time
import pprint
from concurrent.futures import ThreadPoolExecutor, as_completed

import os

# === Load Required Data ===
start_time = time.time()
zcta = gpd.read_file("/home/mmarcial/datafiles/tl_2020_us_zcta520.shp").to_crs(epsg=4326) # reads US tiger shapefiles for ZIP code areas
od_df = pd.read_parquet("/home/mmarcial/datafiles/santa_clara_geoids.parquet") # reads selected county GEOID dataset
od_df['h_geocode'] = od_df['h_geocode'].astype(str)
emissions_df = pd.read_parquet("/home/mmarcial/datafiles/avg_emissions_per_geoid_SantaClara.parquet") # reads avg emissions per GEOID for selected county dataset
emissions_df['Census Block Group Code'] = emissions_df['Census Block Group Code'].astype(str)
print(f"Data loading time: {time.time() - start_time:.2f} seconds") # output loading time for data, can be commented out 

# === Emissions calculation per OD pair ===
def process_route(idx_row):
    idx, row = idx_row
    # Extract origin and destination coordinates from OD dataframe row
    origin = (row['home_lat'], row['home_lon'])
    destination = (row['work_lat'], row['work_lon'])

    # Takes first 12 digits of h_geocode
    origin_tract = row['h_geocode'][:12]

    # Filter emissions_df for a matching GEOID
    emission_row = emissions_df[emissions_df['Census Block Group Code'] == origin_tract]
    if emission_row.empty:
        return {'route_idx': idx, 'error': f"No emissions data for GEOID {origin_tract}"}
    emissions = emission_row.iloc[0]
    
    try:
        # OSRM request URL with origin and destination coordinates and server call 
        osrm_url = f"http://localhost:5000/route/v1/driving/{origin[1]},{origin[0]};{destination[1]},{destination[0]}?overview=full&geometries=polyline"
        response = requests.get(osrm_url)
        route_data = response.json()

        if 'routes' not in route_data or not route_data['routes']:
            return {'route_idx': idx, 'error': "OSRM routing failed"}

        # Decode polyline geometry into a list of latitude and longitude points
        route_coords = polyline.decode(route_data['routes'][0]['geometry'])

        # List of route segments is built
        segments = []
        for a, b in zip(route_coords[:-1], route_coords[1:]):
            distance = geodesic(a, b).miles
            midpoint_geom = Point((a[1] + b[1]) / 2, (a[0] + b[0]) / 2)
            segments.append({'geometry': midpoint_geom, 'distance_miles': distance})

        if not segments:
            return {'route_idx': idx, 'error': "No route segments"}

        seg_df = pd.DataFrame(segments)
        seg_gdf = gpd.GeoDataFrame(data=seg_df, geometry='geometry', crs='EPSG:4326')

        # Assigns a ZIP code to each segment point 
        seg_gdf = gpd.sjoin(seg_gdf, zcta[['ZCTA5CE20', 'geometry']], how='left', predicate='within')
        seg_gdf = seg_gdf.dropna(subset=['ZCTA5CE20'])

        # Aggregate total travel miles per ZIP
        zip_dist = seg_gdf.groupby('ZCTA5CE20')['distance_miles'].sum().reset_index()

        # Multiply distance by per-mile emission factors to get emissions on each ZIP
        pollutants = ['PM25_per_mile', 'SOx_per_mile', 'NOX_per_mile',
                      'VOC_per_mile', 'NH3_per_mile', 'CO2_per_mile']
        for pollutant in pollutants:
            zip_dist[f'{pollutant}_emissions'] = zip_dist['distance_miles'] * emissions[pollutant]

        # Finds route origin ZIP 
        origin_point = Point(origin[1], origin[0])
        origin_zip_match = zcta[zcta.contains(origin_point)]
        origin_zip = origin_zip_match.iloc[0]['ZCTA5CE20'] if not origin_zip_match.empty else None

        # Finds route destination ZIP 
        dest_point = Point(destination[1], destination[0])
        dest_zip_match = zcta[zcta.contains(dest_point)]
        dest_zip = dest_zip_match.iloc[0]['ZCTA5CE20'] if not dest_zip_match.empty else None

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

        # Return result with origin/destination ZIP and emissions breakdown
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
with ThreadPoolExecutor(max_workers=2) as executor: # number of max workers can be changed as needed, through my tests there tended to be errors when using more then 2 workers in the cluster
    #futures = [executor.submit(process_route, item) for item in od_df.head(2000).iterrows()] uncomment if want to run for less OD pairs and change the number in head()
    futures = [executor.submit(process_route, item) for item in od_df.iterrows()] # comment if not running for entire dataset
    for future in as_completed(futures):
        results.append(future.result())

print(f"Total execution time: {time.time() - t_parallel:.2f} seconds")

# === Post-processing and Output Aggregation ===
records_A = []  # Total emissions per ZIP (receptor)
records_B = []  # Emissions caused by origin ZIP
records_C = []  # Emissions caused by destination ZIP
records_D = []  # ZIP-to-ZIP matrix (origin → receptor)

for r in results:
    if 'error' in r or r['origin_zip'] is None or r['dest_zip'] is None:
        continue
    origin_zip = r['origin_zip']
    dest_zip = r['dest_zip']

    for entry in r['emissions_by_zip']:
        receptor_zip = entry['zip']

        # A: Emissions by ZIP as receptor
        records_A.append({
            'zip': receptor_zip,
            **entry
        })

        # B: Origin ZIP causes these emissions
        records_B.append({
            'origin_zip': origin_zip,
            'receptor_zip': receptor_zip,
            **entry
        })

        # C: Destination ZIP causes these emissions
        records_C.append({
            'dest_zip': dest_zip,
            'receptor_zip': receptor_zip,
            **entry
        })

        # D: Matrix of origin → receptor
        #records_D.append({
            #'origin_zip': origin_zip,
            #'receptor_zip': receptor_zip,
            #'PM25': entry['PM25'],
            #'SOx': entry['SOx'],
            #'NOX': entry['NOX'],
            #'VOC': entry['VOC'],
            #'NH3': entry['NH3'],
            #'CO2': entry['CO2']
        #})

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
#if records_D:
#	df_D = pd.DataFrame(records_D).groupby(['origin_zip', 'receptor_zip'])[['PM25', 'SOx', 'NOX', 'VOC', 'NH3', 'CO2']].sum().reset_index()
#else:
#	df_D = pd.DataFrame()

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

save_with_check(df_A, "receptor_zip_emissions.csv", "Total emissions per ZIP (receptor)")
save_with_check(df_B, "origin_zip_emissions.csv", "Total emissions caused by origin ZIP")
save_with_check(df_C, "destination_zip_emissions.csv", "Total emissions caused by destination ZIP")
#save_with_check(df_D, "zip_to_zip_emissions_matrix.csv", "ZIP-to-ZIP emissions matrix")
