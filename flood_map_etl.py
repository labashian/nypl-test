import requests
import sqlite3
import time
import pandas as pd
from shapely.geometry import shape
from sodapy import Socrata


### Retrieves input from the user
class UserInput:
    def __init__(self):
        self.user_borough = None
        self.output = None

    def get_input(self) -> None:
        print("Welcome to the 2100 flood plain building danger tool.\n"), 
        while True:
            print("Select a borough to generate a csv file or sqlite db of buildings in danger of coastal flooding by 2100:\n\n",
                "1) Manhattan \n 2) The Bronx \n 3) Brooklyn \n 4) Queens \n 5) Staten Island \n")
            usr_input = input("Enter 1-5:\n")
            if usr_input.strip() in ["1","2","3","4","5"]:
                self.user_borough = usr_input.strip()
                break
            else:
                print("invalid input, try again.\n")
        
    def num_to_borough(self) -> str:
        borough_name = {
            "1" : "Manhattan",
            "2" : "The_Bronx",    
            "3" : "Brooklyn",
            "4" : "Queens",
            "5" : "Staten_Island"                                            
        }
        return borough_name[self.user_borough]


### Handles extraction from OpenNYC Socrata API
class Extract:
    def __init__(self):
        pass

    # Generic method that work for both flood plain and building data. 
    # Retrieves all buildings (feature code 2100) within user specified
    # borough where eleveation < 20 ft. 
    def get_data(
        self, f_or_b: int = None, 
        borough: str = None, 
        flood_geom = None) -> pd.DataFrame:
        
        client = Socrata("data.cityofnewyork.us", 
                         "aRI2NhyY8j522TEvWAn7fUyS0", #app token
                          timeout=60) 
        offset = 0
        all_results = []

        # Corresponds to borough bin numbers, excluding bins with six trailing 0's, 
        # because API documentation indicates they are unassigned or unknown.
        borough_dict = {
            "1" : "bin >1000000 AND bin < 2000000",
            "2" : "bin >2000000 AND bin < 3000000",
            "3" : "bin >3000000 AND bin < 4000000",
            "4" : "bin >4000000 AND bin < 5000000",
            "5" : "bin >5000000 AND bin < 6000000"
        }
        
        # Case if looking at flood data
        if f_or_b == 0:
            data_code = "rf9r-c4pz" 
            limit = 1
        # Case if looking at housing data
        elif f_or_b == 1:
            data_code = "5zhs-2jue"
            limit = 500
            where = borough_dict[borough] + " AND feature_code = 2100 AND ground_elevation < 20"

        # Loop does pagination to fetch data in chunks
        while True:
            # Sets required arguments for API call
            kwargs = {
                "limit": limit,
                "offset": offset
            }
            # Sets optional arguments for API call
            if f_or_b == 1:
                kwargs["where"] = where
            
            results = client.get(data_code, **kwargs)
            
            if not results:
                break   

            all_results.extend(results)
            
            if f_or_b == 1:
                print(f"Extract: fetched {len(all_results)} buildings so far...",)
            
            if len(results) < limit:
                break
            
            offset += limit
            
        client.close()
        results_df = pd.DataFrame.from_records(all_results)
        return results_df



### Handles Transformations on extracted data.
class Transform:

    def __init__(self):
        pass

    # Checks that no building geometry attributes are null
    def check_null_geom(self, buildings_df: pd.DataFrame) -> pd.DataFrame:
        print("Transform: checking null geometries...\n")
        clean_buildings_df = buildings_df[buildings_df['the_geom'].notna()].copy()
        return clean_buildings_df

    # Does two things: converts elevations from str -> numeric, 
    # and checks for null elevations.
    def check_null_elevation(self, buildings_df: pd.DataFrame) -> pd.DataFrame:
        print("Transform: checking null elevations...\n")
        # Before checking null elevations, converts string to numeric
        buildings_df['ground_elevation'] = pd.to_numeric(
            buildings_df['ground_elevation'],
            errors='coerce'
        )
        elevation_buildings_df = buildings_df[buildings_df['ground_elevation'].notna()].copy()
        return elevation_buildings_df

    # Returns buildings within the 2100 flood plain geometry. 
    def filter_building(self, 
                        flood_df: pd.DataFrame, 
                        clean_buildings_df: pd.DataFrame) -> pd.DataFrame:
        print("Transform: checking intersection of buildings with flood plain map...\n")
        flood_geom = flood_df['the_geom'][0]
        flood_shape = shape(flood_geom)
        flood_buildings_df = clean_buildings_df[
            clean_buildings_df['the_geom'].apply(lambda geom: shape(geom).intersects(flood_shape))
        ].copy()
        return flood_buildings_df

    # Returns df with newly added "flood risk" attribute.
    def risk_percentiles(self, flood_buildings_df: pd.DataFrame) -> pd.Dataframe:
        print("Transform: calculating risk percentiles...\n")
        flood_buildings_df['flood risk'] = pd.qcut(
            flood_buildings_df['ground_elevation'],
            q=3,
            labels=['Highest Risk', 'Medium Risk', 'Lowest Risk']
        )
        return flood_buildings_df

# Handles loading transformed data
class Load:
    def __init__(self):
        pass
        # Exports csv
    def create_csv(self, flood_buildings_df: pd.Dataframe, borough_name: str) -> None:
        flood_buildings_df.to_csv(f"{borough_name}_flood_risk_buildings.csv", index=False)
    
    #with more time, would implement this method
    def create_db(self, flood_buildings_df: pd.DataFrame, borough_name: str )-> None:
        pass


def main():
    ## User Input
    user_input = UserInput()
    user_input.get_input()

    ## Extract
    print("Extract: getting floodplain 2100 data...\n")
    flood_plain_2100_extractor = Extract()
    flood_df = flood_plain_2100_extractor.get_data(f_or_b=0)
    flood_geom = flood_df["the_geom"][0]
    print(f"Extract: getting {user_input.num_to_borough()} buildings...\n")
    buildings_extractor = Extract()
    buildings_df = buildings_extractor.get_data(f_or_b=1, borough=user_input.user_borough)
    
    ## Transform
    transformer = Transform()
    good_geom_df = transformer.check_null_geom(buildings_df)
    good_elevation_df = transformer.check_null_elevation(good_geom_df)
    flood_buildings_df = transformer.filter_building(flood_df, good_elevation_df)
    percentiles_df = transformer.risk_percentiles(flood_buildings_df=flood_buildings_df)
    
    ## Load
    loader = Load()
    loader.create_csv(percentiles_df, user_input.num_to_borough())
    print(f"Load: data outputted to {user_input.num_to_borough()}_flood_risk_buildings.csv")



if __name__ == "__main__":
    main()

