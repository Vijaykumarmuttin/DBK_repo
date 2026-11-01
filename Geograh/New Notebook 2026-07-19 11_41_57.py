# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# COMMAND ----------

base_url = 'https://api.countrystatecity.in'
api_key = 'b3dbf394dfeb7868990a694a1cd239d109384dcf7e05aaa65bf57ff5753a022d'
country = '/v1/countries'
states = '/v1/states'
regions = '/v1/regions'
currency = '/v1/currency'



# COMMAND ----------

# DBTITLE 1,countries_now
popu = 'https://countriesnow.space/api/v0.1/countries/population/cities'
loc = 'https://countriesnow.space/api/v0.1/countries/positions'
curr = 'https://countriesnow.space/api/v0.1/countries/currency'
cap = 'https://countriesnow.space/api/v0.1/countries/capital'
city = 'https://countriesnow.space/api/v0.1/countries'
iso = 'https://countriesnow.space/api/v0.1/countries/iso'
states = 'https://countriesnow.space/api/v0.1/countries/states'


# COMMAND ----------

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

def get_cn_data(
    base:str,
    endpoint:str,
):
    result = requests.get(base+endpoint)
    print(result.status_code)
    if result.status_code == 200:
        data_cn = result.json()
        data = data_cn['data']
        df = spark.createDataFrame(data)
    else:
        print("Error: ", result.status_code)
    return df

# COMMAND ----------

city = get_cn_data(base_cn_url, end_points['cities'])
# city.write.mode("overwrite").saveAsTable("geo_data.landing.city")
# iso = get_cn_data(base_cn_url,)

# COMMAND ----------

population.write.mode("overwrite").saveAsTable("geo_data.landing.population")

# COMMAND ----------

def get_geo_data(
    base:str,
    api_key:str,
    end_point:str,
    table_name:str
    ):
    get_geo_data = requests.get(base+end_point, headers={'X-CSCAPI-KEY': api_key}).json()
    geo_df = spark.createDataFrame(get_geo_data)
    

# COMMAND ----------

kk = requests.get('https://api.countrystatecity.in/v1/states',headers={'X-CSCAPI-KEY': api_key})
print(kk.status_code)

# COMMAND ----------

df = requests.get("https://countriesnow.space/api/v0.1/countries/positions")
print(df.status_code)
# print(df['data'].json())

# Normalize data types before creating DataFrame
data = df.json()['data']
for item in data:
    item['long'] = float(item['long'])
    item['lat'] = float(item['lat'])

df_spark = spark.createDataFrame(data)
display(df_spark)

# COMMAND ----------

# country_data = requests.get(base_url+country, headers={'X-CSCAPI-KEY': api_key}).json()
states_data = requests.get(base_url+states, headers={'X-CSCAPI-KEY': api_key}).json()
# regions_data = requests.get(base_url+regions, headers={'X-CSCAPI-KEY': api_key}).json()
# currency_data = requests.get(base_url+currency, headers={'X-CSCAPI-KEY': api_key}).json()

# COMMAND ----------

# country_df = spark.createDataFrame(country_data)
states_df = spark.createDataFrame(states_data)
# regions_df = spark.createDataFrame(regions_data)
# currency_df = spark.createDataFrame(currency_data)
# display(country_df)
display(states_df)
# display(regions_df)
# display(currency_df)

# COMMAND ----------

country_df.write.mode("overwrite").saveAsTable("geo_data.landing.country")
# states_df.write.mode("overwrite").saveAsTable("states")
# regions_df.write.mode("overwrite").saveAsTable("regions")
# currency_df.write.mode("overwrite").saveAsTable("currency")

# COMMAND ----------

op = "https://www.geonames.org/countries/"
test = requests.get(op)
print(test.status_code)


# COMMAND ----------

print(test.data)
