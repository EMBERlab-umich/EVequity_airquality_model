# The purpose of this file is to take the previously obtained csv file for all of California and filter it so that it only has the GEOIDs of the desired county 
import pandas as pd

# Load GEOID-to-coordinates file
df = pd.read_csv(r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\GEOID_to_Centroid.csv", dtype={'w_geocode': str, 'h_geocode': str})

# Filter for GEOIDs that start with '06085' (Santa Clara County) 
# Can be changed if another county is to be analyzed
sb_df = df[df['w_geocode'].str.startswith('06085')]
sb_df = sb_df[sb_df['h_geocode'].str.startswith('06085')]

sb_df.to_csv("santa_clara_geoids.csv", index=False)

#Optionally give the total number of filtered rows for the chosen County
print(f"Filtered {len(sb_df)} rows for selected County.")