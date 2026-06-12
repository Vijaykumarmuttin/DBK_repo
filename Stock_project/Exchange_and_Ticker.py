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

FMC_base_url = "https://financialmodelingprep.com/api/v3"
FMC_api_key = "glyLZGIF7rZJoHqHiXLfrMcjfvt1nq4J"


# COMMAND ----------

# MAGIC %md
# MAGIC ### aplha

# COMMAND ----------

parameters = {
    "function":"MARKET_STATUS",
    "apikey":alpha_key
}

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
