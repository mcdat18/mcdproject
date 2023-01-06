# Databricks notebook source
# DBTITLE 1,Enrichment Loan Transactions
dbutils.widgets.text('p_file_date', '2022-09-10')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType
#from pyspark.sql.functions import col, sum, avg, max, min
from pyspark.sql.functions import *


# We load customerDrivers data
customerDrivers_df = spark.read.format("delta").load(f"{silver_folder_path}/customerDrivers/date={v_file_date}")

# We load loan transactions data
loanTrx_df = spark.read.format("delta").load(f"{silver_folder_path}/loanTrx/date={v_file_date}")

# We join our two dataframes loan and customerDrivers to enrich our transactional data
featureLoanTrx_df = loanTrx_df.alias('l') \
                        .join(customerDrivers_df.alias('c'), 
                             (col('l.customer_id') == col('c.customer_id')) 
                             & (col('l.date') == col('c.date')), 
                             "inner") \
                        .select(col('l.date'),
                                col('l.customer_id'),
                                col('l.payment_period'),
                                col('l.loan_amount'),
                                col('l.currency_type'),
                                col('l.evaluation_channel'),
                                col('l.interest_rate'),
                                col('c.monthly_salary'),
                                col('c.health_score'),
                                col('c.current_debt'),
                                col('c.category'),
                                col('c.is_risk_customer'))

# We add our insert_timestamp column
featureLoanTrx_df = add_insert_timestamp(featureLoanTrx_df)

# We save our data in delta format in our gold container
# We use replaceWhere option in case we need to re-process our data
featureLoanTrx_df.write.format("delta") \
                       .mode("overwrite") \
                       .partitionBy('date') \
                       .option("replaceWhere", f"date == '{v_file_date}'") \
                       .save(f"{gold_folder_path}/featureLoanTrx")

# We create a table in our demo database so we can query it later
spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS cdt.featureLoanTrx USING DELTA LOCATION '{gold_folder_path}/featureLoanTrx'")



# We are going to do some aggregations from our feature dataframe
aggLoanTrx = featureLoanTrx_df.groupBy('date',
                                       'payment_period',
                                       'currency_type',
                                       'evaluation_channel',
                                       'category') \
                              .agg(
                                sum('loan_amount').alias('sum_loan_amount'),
                                avg('loan_amount').alias('avg_loan_amount'),
                                sum('current_debt').alias('sum_current_debt'),
                                avg('interest_rate').alias('avg_interest_rate'),
                                max('interest_rate').alias('max_interest_rate'),
                                min('interest_rate').alias('min_interest_rate'),
                                avg('health_score').alias('avg_score'),
                                avg('monthly_salary').alias('avg_monthly_salary')) \
                              .orderBy('date', 'payment_period', 'evaluation_channel', 'category', 'currency_type')

# We add our insert_timestamp column
aggLoanTrx = add_insert_timestamp(aggLoanTrx)

# We save our data in delta format in our gold container
# We use replaceWhere option in case we need to re-process our data
aggLoanTrx.write.format("delta") \
                       .mode("overwrite") \
                       .partitionBy('date') \
                       .option("replaceWhere", f"date == '{v_file_date}'") \
                       .save(f"{gold_folder_path}/aggLoanTrx")

# We create a table in our demo database so we can query it later
spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS cdt.aggLoanTrx USING DELTA LOCATION '{gold_folder_path}/aggLoanTrx'")

dbutils.notebook.exit("Success")
