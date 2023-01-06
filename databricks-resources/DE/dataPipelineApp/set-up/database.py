# Databricks notebook source
# DBTITLE 1,Create database
# MAGIC %run "../includes/configurations"

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS cdt CASCADE;")
spark.sql(f"CREATE DATABASE IF NOT EXISTS cdt LOCATION '{gold_folder_path}'")
