# filter_santa_clara_zctas.py
import pandas as pd
import geopandas as gpd
from pathlib import Path

# ======= EDIT THESE =======
COUNTIES_SHP = r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\tl_2020_us_county\tl_2020_us_county.shp"  # Tiger shapefile county
ZCTA_SHP     = r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\nationwide_TIGER\tl_2020_us_zcta520.shp" # Tiger shapefile ZIP
INPUT_DATA   = r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\ACSST5Y2023.S1903_2025-07-28T104140\ACSST5Y2023.S1903-Data.csv" # Census data      
OUTPUT_DATA  = "acs_santa_clara_only.csv"     # .csv or .parquet
# ==========================

STATEFP_CA = "06"
COUNTYFP_SANTA_CLARA = "085"  # 06085, can be changed for desired county

def load_sc_polygon(counties_path: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(counties_path)
    state_col = "STATEFP" if "STATEFP" in gdf.columns else "STATEFP20"
    county_col = "COUNTYFP" if "COUNTYFP" in gdf.columns else "COUNTYFP20"
    sc = gdf[(gdf[state_col] == STATEFP_CA) & (gdf[county_col] == COUNTYFP_SANTA_CLARA)]
    if sc.empty:
        raise ValueError("Santa Clara County not found in the counties shapefile.")
    return sc.dissolve().to_crs(epsg=4326)

def load_zctas(zcta_path: str) -> gpd.GeoDataFrame:
    z = gpd.read_file(zcta_path)
    if "ZCTA5CE20" in z.columns: z = z.rename(columns={"ZCTA5CE20": "ZCTA5"})
    elif "ZCTA5CE10" in z.columns: z = z.rename(columns={"ZCTA5CE10": "ZCTA5"})
    elif "GEOID10"   in z.columns: z = z.rename(columns={"GEOID10": "ZCTA5"})
    elif "GEOID"     in z.columns: z = z.rename(columns={"GEOID": "ZCTA5"})
    else:
        raise ValueError("No ZCTA code column found in ZCTA shapefile.")
    z["ZCTA5"] = z["ZCTA5"].astype(str).str.zfill(5)
    return z.to_crs(epsg=4326)

def extract_zcta_from_geo_id(df: pd.DataFrame) -> pd.DataFrame:
    if "GEO_ID" in df.columns:
        # Keep rows where we can extract a 5-digit ZCTA after 'US'
        df["ZCTA5"] = df["GEO_ID"].astype(str).str.extract(r"US(\d{5})$", expand=False)
        df = df[df["ZCTA5"].notna()].copy()
        df["ZCTA5"] = df["ZCTA5"].str.zfill(5)
        return df

    # Fallbacks: try common ZIP/ZCTA columns or NAME fields
    for col in df.columns:
        if col.lower() in {"zcta5", "zip", "zipcode", "zcta"}:
            df[col] = df[col].astype(str).str.extract(r"(\d{5})", expand=False).str.zfill(5)
            return df.rename(columns={col: "ZCTA5"})
    for col in ("NAME", "NAME_x", "NAME_y"):
        if col in df.columns:
            df["ZCTA5"] = df[col].astype(str).str.extract(r"(\d{5})", expand=False).str.zfill(5)
            return df

    raise ValueError("Couldn't find ZCTA info. Expected GEO_ID or a ZIP/ZCTA column.")

def main():
    # 1) Geographies
    sc_poly = load_sc_polygon(COUNTIES_SHP)
    zctas = load_zctas(ZCTA_SHP)

    # 2) ZCTAs that intersect Santa Clara County
    sc_join = zctas.sjoin(sc_poly[["geometry"]], predicate="intersects", how="inner")
    sc_zips = sorted(sc_join["ZCTA5"].unique().tolist())
    print(f"Found {len(sc_zips)} ZCTAs intersecting Santa Clara County.")

    # 3) ACS data
    in_ext = Path(INPUT_DATA).suffix.lower()
    if in_ext == ".csv":
        df = pd.read_csv(INPUT_DATA, dtype=str)
    elif in_ext in {".parquet", ".pq"}:
        df = pd.read_parquet(INPUT_DATA)
    else:
        raise ValueError("INPUT_DATA must be .csv or .parquet")

    df = extract_zcta_from_geo_id(df)
    before = len(df)
    df = df[df["ZCTA5"].isin(sc_zips)].copy()
    after = len(df)

    # 4) Save outputs
    out_ext = Path(OUTPUT_DATA).suffix.lower()
    if out_ext == ".csv":
        df.to_csv(OUTPUT_DATA, index=False)
    elif out_ext in {".parquet", ".pq"}:
        df.to_parquet(OUTPUT_DATA, index=False)
    else:
        raise ValueError("OUTPUT_DATA must be .csv or .parquet")

    # also save the ZCTA list used (for QA)
    zlist_path = Path(OUTPUT_DATA).with_name(Path(OUTPUT_DATA).stem + "_zctas_used.csv")
    pd.DataFrame({"ZCTA5": sc_zips}).to_csv(zlist_path, index=False)

    print(f"Filtered rows: {after}/{before}")
    print(f"Wrote filtered ACS: {OUTPUT_DATA}")
    print(f"Wrote ZCTA list:    {zlist_path}")

if __name__ == "__main__":
    main()