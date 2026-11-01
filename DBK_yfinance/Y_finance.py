# Databricks notebook source
# MAGIC %run 
# MAGIC /Workspace/Users/vijaykumarmuttin@gmail.com/DBK_yfinance/Auxilary_functions

# COMMAND ----------

msft = get_ticker_spark('MSFT',"2026-01-01","2026-02-01")



