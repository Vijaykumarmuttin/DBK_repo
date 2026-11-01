# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Init

# COMMAND ----------

# MAGIC %run
# MAGIC ./auxilary_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global Parameters

# COMMAND ----------

base_url,api_key = get_api_key('EODH')


# COMMAND ----------

parameters = {
    # "function":"available-exchanges",
    "apikey":api_key
}

# COMMAND ----------

API_KEY = api_key

def get_exchanges(api_key):
    """Fetches all supported global stock exchanges."""
    url = f"https://eodhd.com/api/exchanges-list/?api_token={api_key}&fmt=json"
    response = requests.get(url)
    
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        print("Error fetching exchanges")
        return pd.DataFrame()

# COMMAND ----------

data = get_exchanges(API_KEY)
data.head()


# COMMAND ----------

spark_data = spark.createDataFrame(data)

# COMMAND ----------

def get_listed_companies(exchange_code, api_key):
    """Fetches all listed tickers for a specific exchange code."""
    # We are adding &type=common_stock to filter out ETFs and Mutual Funds
    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange_code}?api_token={api_key}&fmt=json&type=common_stock"
    response = requests.get(url)
    
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        print(f"Error fetching tickers for {exchange_code}")
        return pd.DataFrame()

# COMMAND ----------

dtata = get_listed_companies("NEO",api_key)

# COMMAND ----------

spark_data_stocks_us = spark.createDataFrame(dtata)
df = (
    spark_data_stocks_us.withColumn("key_col",concat_ws("_",col("Code"),col("Exchange")))
)


# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("stocks_aplha.landing.listedStocks")

# COMMAND ----------

display(dtata)

# COMMAND ----------

print(base_url+"/available-exchanges")

# COMMAND ----------

url = ("https://financialmodelingprep.com/stable/available-exchanges?apikey={api_key}")
response = requests.get(url)
data = response.json()
display(data)

# COMMAND ----------

response = requests.get(base_url+"/available-exchanges",params=parameters,timeout=10)
if response.status_code != 200:
    raise Exception(f"Request failed with status code {response.status_code}: {response.text}")
data = response.json()

# COMMAND ----------

response

# COMMAND ----------

# MAGIC %md
# MAGIC ### aplha

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

response = requests.get(aplha_base_url,params=parameters,timeout=10)
data = response.json()

# COMMAND ----------

mkt_data = data["markets"]
df = spark.createDataFrame(mkt_data)
df_exploded = df.withColumn("primary_exchange", explode(split("primary_exchanges", ",\s*")))
display(df_exploded)

# COMMAND ----------

df_exploded.write.format("delta").mode("overwrite").saveAsTable("stocks_aplha.landing.market_status")

# COMMAND ----------

# MAGIC %md
# MAGIC ### EODH exchanes list

# COMMAND ----------

eodh_paramters ={
    "function": "Lexchanges-list",
    "api_token": EODH_API_KEY
}

# COMMAND ----------

result = requests.get(EODH_base_url,params=eodh_paramters,timeout=10)
if result.status_code != 200:
    raise Exception(f"Request failed with status code {result.status_code}")
else:
    print("Request successful")
    data = result.json()
    df = spark.createDataFrame(data)
# df.write.format("delta").mode(

# COMMAND ----------

display(df)

# COMMAND ----------

markt_details = (
    df.withColumn("MIC",explode(split(col("OperatingMIC"),",\s*")))  
)

# COMMAND ----------

markt_details.write.format("delta").mode("overwrite").saveAsTable("stocks_aplha.landing.market_details")

# COMMAND ----------

# MAGIC %md
# MAGIC #### eodh Ticker list

# COMMAND ----------

ticker_paramters ={
    "function": "exchange-symbol-list",
    "code": "LSE",
    "api_token": EODH_API_KEY
}
result_ticker = requests.get(EODH_base_url,params=ticker_paramters,timeout=10)
if result_ticker.status_code != 200:
    raise Exception("Request failed with status code {result_ticker.status_code}")
else:
    print("Request successful")
    data = result_ticker.json()
    df_ticker_us = spark.createDataFrame(data)

# COMMAND ----------

print(result_ticker)

# COMMAND ----------

display(df_ticker)
