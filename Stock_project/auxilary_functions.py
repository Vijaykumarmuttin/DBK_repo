# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import pandas as pd

# COMMAND ----------

api_list = {
    "EODH":{
        "base_url":f"https://eodhd.com/api",
        "api_key":"69cf68088744b7.67862298"
    },
    "alpha":{
        "base_url":f"https://www.alphavantage.co/query",
        "api_key":"B0NYV1KVRDEI5HJM"
    },
    "finnhub":{
        "base_url":f"https://financialmodelingprep.com/stable",
        "api_key":"glyLZGIF7rZJoHqHiXLfrMcjfvt1nq4J"
    }
}

# COMMAND ----------

def get_api_key(API_site:str):
    base_url = api_list[API_site]["base_url"]
    api_key = api_list[API_site]["api_key"]
    return base_url, api_key

# def get_stock_data(api:str,ticket_list:list):
#     base_url, api_key = get_api_key(api)
#     import pandas as pd
#     dfs = {}
#     for ticket in ticket_list:
#         url = f"{base_url}?function=TIME_SERIES_DAILY&symbol={ticket}&apikey={api_key}"
#         df = pd.read_json(url)
#         dfs[ticket] = df
#     return dfs
# def get_stock_data(api:str,ticket_list:list):
#     base_url, api_key = get_api_key(api)
#     url = f"{base_url}?function=TIME_SERIES_DAILY&symbol=IBM&apikey={api_key}"
#     df = pd.read_json(url)
#     return df
   

# COMMAND ----------

def get_exchanges(api_key):
    """Fetches all supported global stock exchanges."""
    url = f"https://eodhd.com/api/exchanges-list/?api_token={api_key}&fmt=json"
    response = requests.get(url)
    
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        print("Error fetching exchanges")
        return pd.DataFrame()
    
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

def convert_to_spark(df: pd.DataFrame):
    """
    Convert a pandas DataFrame to a Spark DataFrame.
    """
    return spark.createDataFrame(df)
def convert_to_pandas(df) -> pd.DataFrame:
    """
    Convert a Spark DataFrame to a pandas DataFrame.
    """
    return df.toPandas()

# COMMAND ----------

def get_listed_companies_spark(exchange_code, api_key):
    df = get_listed_companies(exchange_code, api_key)
    return convert_to_spark(df)

