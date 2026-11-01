# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## global Parameters

# COMMAND ----------

# DBTITLE 1,URLs
popu = 'https://countriesnow.space/api/v0.1/countries/population/cities'
loc = 'https://countriesnow.space/api/v0.1/countries/positions'
curr = 'https://countriesnow.space/api/v0.1/countries/currency'
cap = 'https://countriesnow.space/api/v0.1/countries/capital'
city = 'https://countriesnow.space/api/v0.1/countries'
iso = 'https://countriesnow.space/api/v0.1/countries/iso'
states = 'https://countriesnow.space/api/v0.1/countries/states'


# COMMAND ----------

# DBTITLE 1,cleaned URLs
#### country now
end_points = {
    "states" : "/v0.1/countries/states",
    "currency" : "/v0.1/countries/currency",
    "capitals" : "/v0.1/countries/capital",
    "cities" : "/v0.1/countries",
    "iso" : "/v0.1/countries/iso",
    "population" : "/v0.1/countries/population/cities",
    "positions" : "/v0.1/countries/positions"
}

base_cn_url = "https://countriesnow.space/api"

# COMMAND ----------

catalog = 'geo_data'
schema = 'landing'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions

# COMMAND ----------

def get_cn_data(
    base:str,
    endpoint:str,
):
    """
    Fetches data from the CountriesNow API for the specified endpoint and returns a Spark DataFrame.
    
    Args:
        base (str): Base URL of the CountriesNow API.
        endpoint (str): API endpoint to fetch data from.
    
    Returns:
        pyspark.sql.DataFrame: Spark DataFrame containing the data from the API.
    """
    # Print the full request URL
    print(f"Requesting data from: {base+endpoint}")
    result = requests.get(base+endpoint)
    # Print the HTTP status code of the response
    print(f"HTTP status code: {result.status_code}")
    if result.status_code == 200:
        data_cn = result.json()
        # Print which endpoint is being parsed
        print(f"Parsing data for endpoint: {endpoint}")
        if endpoint =="/v0.1/countries/positions":
            data = data_cn['data']
            # Convert longitude and latitude values to float for positions endpoint
            print("Converting longitude and latitude to float.")
            for item in data:
                item['long'] = float(item['long'])
                item['lat'] = float(item['lat'])
            # Create Spark DataFrame from the processed data
            df = spark.createDataFrame(data)
        else:
            data = data_cn['data']
            # Create Spark DataFrame from the data for other endpoints
            df = spark.createDataFrame(data)
        # Indicate successful DataFrame creation
        print("DataFrame created successfully.")
    else:
        # Print error if request was not successful
        print("Error: ", result.status_code)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## get data

# COMMAND ----------

tables = [
    "states",
    "currency",
    "capitals",
    "cities",
    "iso",
    "population",
    "positions"
]
for tab in tables:
    if "posistions" in tab:
        tab = "positions"
    df = get_cn_data(base_cn_url, end_points[tab])
    df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{tab}")
