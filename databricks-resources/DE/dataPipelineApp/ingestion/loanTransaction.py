# Databricks notebook source
# DBTITLE 1,Processing loan transaction data
dbutils.widgets.text('p_file_date', '2022-09-10')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType

schema = StructType(fields=[
    StructField("date", DateType(), False),
    StructField("customerId", StringType(), False),
    StructField("paymentPeriod", IntegerType(), False),
    StructField("loanAmount", DoubleType(), False),
    StructField("currencyType", StringType(), False),
    StructField("evaluationChannel", StringType(), False),
    StructField("interest_rate", DoubleType(), False)
])

loanTrx_df = spark.read. \
                option("header", True). \
                schema(schema). \
                csv(f"{bronze_folder_path}/transactions/loanTrx_{v_file_date}.csv")

# As this data is supossed to come from a core application, we would just renamed a column
final_df = loanTrx_df \
               .withColumnRenamed("customerId", "customer_id") \
               .withColumnRenamed('paymentPeriod' , 'payment_period') \
               .withColumnRenamed('loanAmount' , 'loan_amount') \
               .withColumnRenamed('currencyType' , 'currency_type') \
               .withColumnRenamed('evaluationChannel' , 'evaluation_channel') 

# We save our data in delta format in our silver container
# We use replaceWhere option in case we need to re-process our data
final_df.write.format("delta") \
              .mode("overwrite") \
              .partitionBy('date') \
              .option("replaceWhere", f"date == '{v_file_date}'") \
              .save(f"{silver_folder_path}/loanTrx")

dbutils.notebook.exit("Success")
