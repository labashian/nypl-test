import requests
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Dict
from sodapy import Socrata


class Extractor:
    def __init__(self):
        pass

    def get_data(self, url: str, data_code: str, limit: int) -> pd.DataFrame:
        client = Socrata(url, None)
        results = client.get(data_code, limit=limit)
        results_df = pd.DataFrame.from_records(results)
        return results_df
        

class Pipeline:

    def __init__(self, url):
        self.url = url


    # returns api
    def get_data(self):
        return 


def main():
    buildings_extractor = Extractor()
    buildings_df = buildings_extractor.get_data(url="data.cityofnewyork.us", data_code="5zhs-2jue", limit=2000)
    print("buildings_df = ", buildings_df)
    flood_plain_2100_extractor = Extractor()
    flood_df = flood_plain_2100_extractor.get_data(url="data.cityofnewyork.us", data_code="rf9r-c4pz", limit=2000 )
    print("flood_df = ", flood_df)


if __name__ == "__main__":
    main()

