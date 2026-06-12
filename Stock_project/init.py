# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## Install the required libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import pandas as pd

# COMMAND ----------

alpha_key = "B0NYV1KVRDEI5HJM"
base_url = f"https://www.alphavantage.co/query"

# COMMAND ----------

params = {
                'function': 'TIME_SERIES_WEEKLY_ADJUSTED',
                'symbol': "IBM",
                # 'interval': '1min',
                'apikey': alpha_key
            }


# COMMAND ----------

# DBTITLE 1,Fetch intraday stock data with requests
response = requests.get(base_url, params=params, timeout=10)
data = response.json()

# COMMAND ----------

dat = data['Weekly Adjusted Time Series']
df = pd.DataFrame(dat.items(), columns=['Date', 'Data'])

df = df.explode('Data')



# COMMAND ----------

display(df)
