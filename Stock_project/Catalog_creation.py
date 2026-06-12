# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Create catalog Stocks_aplha
spark.sql("CREATE CATALOG IF NOT EXISTS Stocks_aplha")

# Create schemas for medallion architecture
for layer in ["Bronze", "Silver", "Gold", "Landing"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS Stocks_aplha.{layer}")
