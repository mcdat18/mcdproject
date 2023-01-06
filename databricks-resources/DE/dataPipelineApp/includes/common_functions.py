# Databricks notebook source
# DBTITLE 1,Common functions
from pyspark.sql.functions import current_timestamp

# This function receives a dataframe as input and add a insert_timestamp column
def add_insert_timestamp(input_df):
    output_df = input_df.withColumn('insert_timestamp', current_timestamp())
    return output_df
