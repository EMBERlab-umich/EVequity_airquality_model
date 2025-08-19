#This script will serve to treat the fleet dataset for the county of santa clara in order to begin our toy model
#This will allow for the fuel collumn to be generated so that this can be crossed with the summed EMFAC dataset
import pandas as pd

# Path to fleet database file
file_path = r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\FleetDB-County-SANTACLARA-2022-P_T1-GVWR-All-All-Agg-Selected_Model_Years-All-ByCensusBlockGroupCode.csv"

# Skip metadata to load actual data 
# might have to be changed depending on the datafile being used, printing the df might assist with debugging if needed to estimate correct number of rows to skip 
df = pd.read_csv(file_path, skiprows=12)

# Filter out Hydrogen and Natural Gas from fuel types as they are not rpesent in the emmissions dataset
df = df[~df['Fuel Type'].isin(['Hydrogen', 'Natural Gas'])].copy()

# Add the correct names to the fuel collumn so the fleet data can be merged with the emissions data
def classify_fuel(row):
    fuel_type = row['Fuel Type']
    fuel_tech = row['Fuel Technology']
    
    if fuel_type == 'Gasoline':
        if fuel_tech == 'ICE':
            return 'Gasoline'
        elif fuel_tech == 'PHEV':
            return 'Plug-in Hybrid'
    elif fuel_type == 'Electric':
        return 'Electricity'
    elif fuel_type == 'Diesel':
        return 'Diesel'
    return None

# Create fuel collumn
df['fuel'] = df.apply(classify_fuel, axis=1)

# Save result
df.to_csv("cleaned_fleet_data.csv", index=False)

print("Cleaned data saved as 'cleaned_fleet_data.csv'")