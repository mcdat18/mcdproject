# Databricks notebook source
# DBTITLE 1,Processing customer data
dbutils.widgets.text('p_file_date', '2022-09-10')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Import Libraries
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType
from pyspark.sql.functions import substring

# Declaring schema
schema = StructType(fields=[
    StructField("customerId", StringType(), False),
    StructField("firstName", StringType(), False),
    StructField("lastName", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

# Reading raw customer data
customer_df = spark.read. \
                option("header", True). \
                schema(schema). \
                csv(f"{bronze_folder_path}/customer/customer_{v_file_date}.csv")

# Renaming some columns
tmp_customer_df = customer_df. \
                       withColumnRenamed("customerId", "customer_id"). \
                       withColumnRenamed("firstName", "first_name"). \
                       withColumnRenamed("lastName", "last_name")

# Transforming gender column
tmp_customer_df = tmp_customer_df. \
                        withColumn('gender', substring('gender', 1,1))

# We add an insert timestamp column to our dataframe
final_df = add_insert_timestamp(tmp_customer_df)

# We save our data in delta format in our silver container
final_df.write.mode("overwrite").format("delta").save(f"{silver_folder_path}/customer")

# We print a message if everything runs successfully
dbutils.notebook.exit("Success")
