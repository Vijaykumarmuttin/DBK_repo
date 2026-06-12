# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

api_list = {
    "base_url" : "",
    "api_key" : ""
}
