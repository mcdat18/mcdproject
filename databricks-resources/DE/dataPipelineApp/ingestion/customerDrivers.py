# Databricks notebook source
# DBTITLE 1,Processing customer driver data
dbutils.widgets.text('p_file_date', '2022-09-10')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col

schema = StructType(fields=[
    StructField("date", DateType(), False),
    StructField("customerId", StringType(), False),
    StructField("monthly_salary", DoubleType(), True),
    StructField("health_score", IntegerType(), True),
    StructField("current_debt", DoubleType(), True),
    StructField("category", StringType(), True),
])

customerDrivers_df = spark.read. \
                        option("header", True). \
                        schema(schema). \
                        csv(f"{bronze_folder_path}/customerDriver/customerDrivers_{v_file_date}.csv")

# We impute some values to our columns
# For customer with null monthly salary, we set the minimum vital salary (example: 1500)
# As a rule on the bank customers who don't have score yet, must have 100 of health score
# If current debt is null set to 0
tmp_customerDrivers_df = customerDrivers_df.fillna({'monthly_salary':1500,
                                                    'health_score':100,
                                                    'current_debt': 0,
                                                    'category': 'OTHERS'})
                                    
# We rename customerId column
# We add a flag to know which customer are risky according to their score
final_df = tmp_customerDrivers_df. \
                    withColumnRenamed('customerId', 'customer_id'). \
                    withColumn('is_risk_customer', col('health_score') < 100)



# We save our data in delta format in our silver container
# We use replaceWhere option in case we need to re-process our data
final_df.write.format("delta") \
              .mode("overwrite") \
              .partitionBy('date') \
              .option("replaceWhere", f"date == '{v_file_date}'") \
              .save(f"{silver_folder_path}/customerDrivers")

dbutils.notebook.exit("Success")
