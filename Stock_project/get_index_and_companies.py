# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run
# MAGIC ./auxilary_functions

# COMMAND ----------

exchange_df = spark.sql("select * from stocks_aplha.bronze.indexedMarkets")

# COMMAND ----------

list_20 = (
    exchange_df
    .where("extracted_flag = 0")
    .limit(5)
    .withColumn("extracted_flag", lit(1))
)

# COMMAND ----------

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stocks_aplha.bronze.indexedMarkets")
