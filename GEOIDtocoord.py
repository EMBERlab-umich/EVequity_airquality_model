#The purpose of this code is to read the LODES data as well as tiger shapefiles and create a file that relates all California GEOIDs (15 digit) to the coordinates of their centroids
import geopandas as gpd
import pandas as pd

# The blocks variable will read the tiger shapefile for the state of California, of any other desired location
blocks = gpd.read_file(r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\tl_2020_06_tabblock20\tl_2020_06_tabblock20.shp")

# Then, a collumn containing a 15 digit GEOID will be generated to allow for later crossing between the LODES and tiger shapefiles
blocks['GEOID'] = (
    blocks['STATEFP20'] +
    blocks['COUNTYFP20'] +
    blocks['TRACTCE20'] +
    blocks['BLOCKCE20']
)

# Keep only GEOID and geometry collumns of the dataset 
blocks = blocks[['GEOID', 'geometry']]
blocks = blocks.to_crs(epsg=4326) # change the geometry collumn to standard coordinate reference system 4326
# debugging print statement, ignore if not necessary 

#print(blocks.crs)

# create a new collumn to blocks that has the coordinates of the centroid of each GEOID and then drop the original geometry collumn
blocks['centroid'] = blocks.geometry.centroid
blocks = blocks.drop(columns='geometry')
# debugging print statement, ignore if not necessary 

#print(blocks)


file_path = r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\ca_od_main_JT00_2022.csv.gz" #read LODES dataset for all commuters in california
# Change geocode columns to strings in order to prevent errors
dtype_spec = {
    'w_geocode': str,
    'h_geocode': str
}


# assign LODES data to a dataframe 
LODES_df = pd.read_csv(file_path, compression='gzip',usecols = ['w_geocode', 'h_geocode'], dtype=dtype_spec)

# Create collumns for both the x and y coordinates of the latitudes of each GEOID centroid 
blocks['lat'] = blocks['centroid'].y
blocks['lon'] = blocks['centroid'].x

# merge both dataframes to add the latitude and longitude coordinates of each home GEOID
LODES_df = LODES_df.merge(
    blocks.rename(columns={'GEOID': 'h_geocode', 'lat': 'home_lat', 'lon': 'home_lon'}),
    on='h_geocode',
    how='left'
)

# merge both dataframes to add the latitude and longitude coordinates of each work GEOID
LODES_df = LODES_df.merge(
    blocks.rename(columns={'GEOID': 'w_geocode', 'lat': 'work_lat', 'lon': 'work_lon'}),
    on='w_geocode',
    how='left'
)

# cleans the final dataframe and converts it to a .csv file which will be used in other files 
LODES_df = LODES_df.drop(columns=['centroid_x','centroid_y'])
LODES_df.to_csv(r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\GEOID_to_Centroid.csv", index=False)
