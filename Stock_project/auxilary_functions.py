# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

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
   
