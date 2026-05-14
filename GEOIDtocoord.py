#The purpose of this code is to read the LODES data as well as tiger shapefiles and create a file that relates all California GEOIDs (15 digit) to the coordinates of their centroids
import geopandas as gpd
import pandas as pd

# The blocks variable will read the tiger shapefile for the state of California, of any other desired location
blocks = gpd.read_file("data/tl_2023_06_tabblock20/tl_2023_06_tabblock20.shp")

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


file_path = "data/LODES_data_cars.csv" #read LODES dataset for all commuters in california (edited to include number of cars)
# Change geocode columns to strings in order to prevent errors
# Note that this edited version of the LODES database has removed the zeroes from the front of the strings. If using a different data source, this may need to be addressed in this file and in the filtering file.
dtype_spec = {
    'w_geocode': str,
    'h_geocode': str,
    'Number of Cars': float
}


# assign LODES data to a dataframe 
LODES_df = pd.read_csv(file_path, usecols = ['w_geocode', 'h_geocode','Number of Cars'], dtype=dtype_spec)
LODES_df['w_geocode'] = LODES_df['w_geocode'].str.zfill(15)
LODES_df['h_geocode'] = LODES_df['h_geocode'].str.zfill(15)

# Create collumns for both the x and y coordinates of the latitudes of each GEOID centroid 
blocks['lat'] = blocks['centroid'].y
blocks['lon'] = blocks['centroid'].x

# merge both dataframes to add the latitude and longitude coordinates of each home GEOID
LODES_df = LODES_df.merge(
    blocks.rename(columns={'GEOID': 'h_geocode', 'lat': 'home_lat', 'lon': 'home_lon'}),
    on='h_geocode',
    how='left'
)

print(LODES_df.head())

# merge both dataframes to add the latitude and longitude coordinates of each work GEOID
LODES_df = LODES_df.merge(
    blocks.rename(columns={'GEOID': 'w_geocode', 'lat': 'work_lat', 'lon': 'work_lon'}),
    on='w_geocode',
    how='left'
)

# cleans the final dataframe and converts it to a .csv file which will be used in other files 
LODES_df = LODES_df.drop(columns=['centroid_x','centroid_y'])
LODES_df.to_csv("data/GEOID_to_Centroid.csv", index=False)
