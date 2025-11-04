# NYPL Data Engineer Coding Assessment: 2100 Flood Plain Data and NYC Buildings

## Data Sources:
 [NYC Building Footprints](https://data.cityofnewyork.us/City-Government/Building-Footprints-Map-/jh45-qr5r)
 [2100 100-year Floodplain] (https://data.cityofnewyork.us/Environment/Sea-Level-Rise-Maps-2100-100-year-Floodplain-/rf9r-c4pz/about_data)

## Running the pipeline
I used [uv] (https://docs.astral.sh/uv/) as my package manager and virtual environment, in part due to positive personal experience and recent positive coverage of the tool: https://emily.space/posts/251023-uv

In order to run the pipeline, please follow the install instructions for uv. 

## Purpose
I decided to combine two map data sets to create a dataset that would enable a user to create a map that could visualize and quantify the relative risk to buildings in a chosen borough of New York City.

## Method
I accomplished this through a simple user interface system, where the user selects which borough they would like to examine. Once the user selects the borough, this pipeline then extracts data from the NYC OpenData/Socrata API, but limits extracted buildings to ones in a particular borough based on the "bin" attribute, as well as buildings with a "ground_elevation" attribute value of less than 20 ft. Then using pandas and shapely, the GeoJSON of each building in this set is compared against the GeoJSON of the 2100 100-year flood plain, and buildings that do not intersect are filtered out. Finally, using the qcut method from pandas, the buildings are separated into 3 equal percentiles based on elevation and assigned "Highest Risk", "Medium Risk", or "Lowest Risk."

## Limitations and Potential Improvements
With more time, I would give the user more control over how they would like to split of the percentiles. I would also consider a more advanced algorithm/ calculation than just risk based on elevation, as there are obviously many other variables like distance from cost, elevation relative to surroundings, etc., that can factor into flooding risk. 

Currently, I have hardcoded an app token to allow for faster API speeds, but in a real production environment I would avoid doing so for security. For the sake of simplicity of this exercise I am leaving it as is for now.

I would like to optimize fetching speed from the API, as currently it seems a little slow. 

I would implement exporting to a SQL-based database.

I would implement robust testing on all parts of the pipeline. 

## Sample Output
Please examine "Brooklyn_flood_risk_buildings.csv" for a sample output from this pipeline. 