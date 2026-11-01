# Databricks notebook source
# MAGIC %pip install yfinance

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import yfinance as yf
import pandas as pd

# COMMAND ----------

def download_ticker_data(ticker:str, start:str, end:str) -> pd.DataFrame:
    """
    Get the historical data for a given ticker symbol from Yahoo Finance.

    Parameters:
    ticker (str): The ticker symbol of the stock.
    start (str): The start date in 'YYYY-MM-DD' format.
    end (str): The end date in 'YYYY-MM-DD' format.

    Returns:
    pd.DataFrame: A DataFrame containing the historical data for the specified ticker.
    """
    return yf.download(ticker, start=start, end=end)

# COMMAND ----------

def generate_clean_df(dfs: dict) -> pd.DataFrame:
    """
    Create a tidy DataFrame from a dictionary of ticker DataFrames.
    Works for both simple columns and MultiIndex columns.
    """
    frames = []

    for ticker, data in dfs.items():
        data = data.copy().reset_index()

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [
                col[0] if isinstance(col, tuple) else str(col)
                for col in data.columns
            ]
        else:
            data.columns = [str(col) for col in data.columns]

        data["ticker"] = ticker
        frames.append(data)

    return pd.concat(frames, ignore_index=True)

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

def get_ticker_spark(ticker:str, start:str, end:str):
    """
    Clean and tidy the downloaded ticker data for Spark conversion.

    Parameters:
    ticker (str): The ticker symbol.
    df (pd.DataFrame): The raw historical data for the ticker.

    Returns:
    pd.DataFrame: A tidy DataFrame with a 'ticker' column.
    """
    df = download_ticker_data(ticker, start, end)
    df = generate_clean_df({ticker: df})
    df = convert_to_spark(df)
    return df

