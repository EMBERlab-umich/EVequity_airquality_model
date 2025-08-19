#The purpose of this code will be to take the original emissions rate code and modify it in a way that allows for 
#emissions to be attributed to each ZIP code

import pandas as pd

# Load fleet data 
fleet_df = pd.read_csv("cleaned_fleet_data.csv")
fleet_df = fleet_df.rename(columns={"fuel": "Fuel"})

# Ensure model year is numeric
fleet_df['Model Year'] = fleet_df['Model Year'].astype(int)

# Load EMFAC emissions data 
emfac_path = r"C:\Users\marco\OneDrive\Documents\ESENG masters\ESENG 503 2\EMFAC.csv"
EMFAC_df = pd.read_csv(emfac_path, skiprows=range(0, 8))

# Calculate emissions per mile in kg 
PM25_per_mile = EMFAC_df['PM2.5_TOTAL']*1000 / EMFAC_df['Total VMT']
SOx_per_mile = EMFAC_df['SOx_TOTEX']*1000 / EMFAC_df['Total VMT']
NOX_per_mile = EMFAC_df['NOx_TOTEX']*1000 / EMFAC_df['Total VMT']
VOC_per_mile = EMFAC_df['ROG_TOTAL']*1000 / EMFAC_df['Total VMT']
NH3_per_mile = EMFAC_df['NH3_RUNEX']*1000 / EMFAC_df['Total VMT']
CO2_per_mile = EMFAC_df['CO2_TOTEX']*1000 / EMFAC_df['Total VMT']

# create emissions dataframe
selected_cols = ['Vehicle Category', 'Model Year', 'Fuel']
EMFAC_new = EMFAC_df[selected_cols].copy()
EMFAC_new = EMFAC_new.assign(
    PM25=PM25_per_mile,
    SOx=SOx_per_mile,
    NOX=NOX_per_mile,
    VOC=VOC_per_mile,
    NH3=NH3_per_mile,
    CO2=CO2_per_mile
)

# Debug print statements, uncomment in case needed
# print(EMFAC_new)
# print(fleet_df)
# print('a')
# print(fleet_df['Census Block Group Code'].nunique())
# print('b')

# Groups vehicles together if they have the same GEOID, Fuel, model year and vehicle category and sums their vehicle population in a new collumn  called vehicle count 
group_cols = ['Census Block Group Code', 'Fuel', 'Model Year', 'Vehicle Category']
fleet_counts = fleet_df.groupby(group_cols)['Vehicle Population'].sum().reset_index(name='vehicle_count')

# Correct the vehicle categories in EMFAC_new so that they match with fleet counts
EMFAC_new['Vehicle Category'] = EMFAC_new['Vehicle Category'].replace({
    'LDA': 'P',
    'LDT1': 'T1'
})

# optional print statement
#print(fleet_counts)

# Merge Fleet Counts with Emission Rates 
merged_df = fleet_counts.merge(
    EMFAC_new,
    on=['Fuel', 'Model Year', 'Vehicle Category'],
    how='left'
)

# optional print statements
# print(merged_df)
# print(fleet_df['Model Year'].unique())
# print(EMFAC_new['Model Year'].unique())
# print(fleet_df['Fuel'].unique())
# print(EMFAC_new['Fuel'].unique())

# Compute Weights and Weighted Emissions
merged_df['total'] = merged_df.groupby('Census Block Group Code')['vehicle_count'].transform('sum')
merged_df['weight'] = merged_df['vehicle_count'] / merged_df['total']

for col in ['PM25', 'SOx', 'NOX', 'VOC', 'NH3', 'CO2']:
    merged_df[f'{col}_weighted'] = merged_df['weight'] * merged_df[col]

# Average emissions are joined per GEOID
emission_cols = [f'{col}_weighted' for col in ['PM25', 'SOx', 'NOX', 'VOC', 'NH3', 'CO2']]
avg_emissions_per_geoid = merged_df.groupby('Census Block Group Code')[emission_cols].sum().reset_index()

# Rename columns 
avg_emissions_per_geoid = avg_emissions_per_geoid.rename(columns={
    'PM25_weighted': 'PM25_per_mile',
    'SOx_weighted': 'SOx_per_mile',
    'NOX_weighted': 'NOX_per_mile',
    'VOC_weighted': 'VOC_per_mile',
    'NH3_weighted': 'NH3_per_mile',
    'CO2_weighted': 'CO2_per_mile'
})

# Results are saved to a csv
avg_emissions_per_geoid.to_csv("avg_emissions_per_geoid_SantaClara.csv", index=False)

print("Average emissions saved to 'avg_emissions_per_geoid_SantaClara.csv'")