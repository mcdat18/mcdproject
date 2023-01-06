# Databricks notebook source
# DBTITLE 1,Enrichment Customer
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Importing required libraries
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType
from delta.tables import DeltaTable

# Reading delta data from silver container
customer_df = spark.read.format("delta").load(f"{silver_folder_path}/customer")

# Selecting required columns
tmp_customer = customer_df.select('customer_id',
                                  'first_name',
                                  'last_name',
                                  'phone',
                                  'email',
                                  'gender',
                                  'is_active')


# Adding insert timestamp column
tmp_customer = add_insert_timestamp(tmp_customer)

# APPLYING / USING UPSERT TECHNIQUE BECAUSE CUSTOMER IS A MASTER TABLE
# Before we made and upsert, we need to validate that the delta table already exists 
# To solve this scenario we added an if condition that validates if there is data on the container
# If data not exists, it's the first time we are going to write it to the gold container in delta format

folder_path = f"{gold_folder_path}/customer"

if (DeltaTable.isDeltaTable(spark, folder_path)):
    deltaTable = DeltaTable.forPath(spark, folder_path)
    dfUpdates = tmp_customer
    
    deltaTable.alias('src') \
      .merge(
        dfUpdates.alias('upd'),
        "src.customer_id = upd.customer_id"
      ) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
else:
    tmp_customer.write.mode("overwrite").format("delta").save(folder_path)

# Create master customer table on our database
spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS cdt.customer USING DELTA LOCATION '{gold_folder_path}/customer'")

# Display success message
dbutils.notebook.exit("Success")
