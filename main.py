import requests
import sqlite3
import pandas as pd
from shapely.geometry import shape
from sodapy import Socrata


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

    def csv_or_db(self) -> None:
        print("Export data to csv or SQLite database?")
        while True:
            usr_input = input("1) csv\n2) database \n")
            if usr_input.strip() in ["1", "2"]:
                self.output = usr_input.strip()
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

## Extracts the 2100 flood plain multipoylgon and filters buildings in a 
#   chosen borough using "intersect" 
## from Socrata API
## Perform null checks and other data cleaning strategies at this stage. type checking, null checking, others? 
class Extract:
    def __init__(self):
        pass


    def get_data(self, f_or_b: int = None, borough: str = None, flood_geom = None) -> pd.DataFrame:
        

        client = Socrata("data.cityofnewyork.us", "aRI2NhyY8j522TEvWAn7fUyS0") #second param is app token
        offset = 0
        all_results = []

        # Corresponds to borough bin numbers, exlcuding bins with six trailing 0's, 
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

        # Loop does pagination on called data for efficient data extraction
        while True:
            # sets required arguments for API call
            kwargs = {
                "limit": limit,
                "offset": offset
            }
            # sets optional arguments for API call
            if f_or_b == 1:
                kwargs["where"] = where
            
            results = client.get(data_code, **kwargs)
            
            if not results:
                break   

            all_results.extend(results)

            if len(results) < limit:
                break
            
            offset += limit
            
        client.close()
        results_df = pd.DataFrame.from_records(all_results)
        return results_df



# transform the remaining buildings from the extract sequence, and find equal thirds
# percentiles 
# utilize pd.qcut
class Transform:

    def __init__(self):
        pass

    def check_null_geom(self, buildings_df) -> pd.DataFrame:
        print("Transform: checking null geometries...\n")
        clean_buildings_df = buildings_df[buildings_df['the_geom'].notna()].copy()
        return clean_buildings_df

    def check_null_elevation(self, buildings_df: pd.DataFrame) -> pd.DataFrame:
        print("Transform: checking null elevations...\n")
        # Before checking null elevations, converts string to numeric
        buildings_df['ground_elevation'] = pd.to_numeric(
            buildings_df['ground_elevation'],
            errors='coerce'
        )
        elevation_buildings_df = buildings_df[buildings_df['ground_elevation'].notna()].copy()
        return elevation_buildings_df

    # returns cleaned and filtered buildings. Checks for null values and finds intersection with flood plain 2100
    def filter_building(self, flood_df: pd.DataFrame, clean_buildings_df: pd.DataFrame) -> pd.DataFrame:
        print("Transform: checking intersection of buildings with flood plain map...\n")
        flood_geom = flood_df['the_geom'][0]
        flood_shape = shape(flood_geom)
        

        flood_buildings_df = clean_buildings_df[
            clean_buildings_df['the_geom'].apply(lambda geom: shape(geom).intersects(flood_shape))
        ].copy()
        return flood_buildings_df

    def risk_percentiles(self, flood_buildings_df: pd.DataFrame):
        print("Transform: calculating risk percentiles...\n")
        flood_buildings_df['flood risk'] = pd.qcut(
            flood_buildings_df['ground_elevation'],
            q=3,
            labels=['Highest Risk', 'Medium Risk', 'Lowest Risk']
        )
        return flood_buildings_df

# load into a SQLite database and/or a csv file.
class Load:
    def __init__(self):
        pass

    def create_csv(self, flood_buildings_df: pd.Dataframe, borough_name: str) -> None:
        flood_buildings_df.to_csv(f"{borough_name}_flood_risk_buildings.csv", index=False)

    def create_db(self, flood_buildings_df: pd.DataFrame, borough_name: str )-> None:
        pass


def main():
    ## User Input
    user_input = UserInput()
    user_input.get_input()

    ## Extract
    print("Extract: getting fload plain 2100 data...\n")
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
    loader.create_csv(percentiles_df, user_input.user_borough)
    print(f"Load: data outputted to {user_input.num_to_borough()}_flood_risk_buildings.csv")



if __name__ == "__main__":
    main()

