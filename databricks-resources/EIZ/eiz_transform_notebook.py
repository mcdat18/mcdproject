# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,array,ArrayType,DateType,TimestampType
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
import hashlib
import datetime
import urllib.request
import json
from datetime import timedelta, date

# COMMAND ----------

# DBTITLE 1,Parameters will be received from Azure Data Factory workflow
STORAGE_ACCOUNT=dbutils.widgets.get("STORAGE_ACCOUNT")
ADLS_KEY=dbutils.widgets.get("ADLS_KEY")
BRONZE_LAYER_NAMESPACE=dbutils.widgets.get("BRONZE_LAYER_NAMESPACE")
SILVER_LAYER_NAMESPACE=dbutils.widgets.get("SILVER_LAYER_NAMESPACE")
STORE_SALES_FOLDER=dbutils.widgets.get("STORE_SALES_FOLDER")
ADLS_FOLDER=dbutils.widgets.get("ADLS_FOLDER")
TABLE_LIST=dbutils.widgets.get("TABLE_LIST")
CURRENCY_LIST=dbutils.widgets.get("CURRENCY_LIST")
CURRENCY_FOLDER=dbutils.widgets.get("CURRENCY_FOLDER")
GEOLOCATION_FOLDER=dbutils.widgets.get("GEOLOCATION_FOLDER")
LOGS_FOLDER=dbutils.widgets.get("LOGS_FOLDER")
ECOMM_FOLDER=dbutils.widgets.get("ECOMM_FOLDER")

UPDATED=datetime.datetime.today().replace(second=0, microsecond=0)
spark.conf.set("fs.azure.account.key."+STORAGE_ACCOUNT+".blob.core.windows.net", ADLS_KEY)

# COMMAND ----------

# DBTITLE 1,Define Schemas for all files
CUSTOMERS_SCHEMA =[
    ('customer_id', IntegerType()),
    ('customer_name', StringType()),
    ('address', StringType()),
    ('city', StringType()),
    ('postalcode', StringType()),
    ('country', StringType()),
    ('phone', StringType()),
    ('email', StringType()),
    ('credit_card', StringType()),
    ('updated_at', TimestampType())
]

ORDERS_SCHEMA =[
    ('order_number', IntegerType()),
    ('customer_id', IntegerType()),
    ('product_id', IntegerType()),
    ('order_date', DateType()),
    ('units', IntegerType()),
    ('sale_price', FloatType()),
    ('currency', StringType()),
    ('order_mode', StringType()),
    ('sale_price_usd', FloatType()),
    ('updated_at', TimestampType())
]

PRODUCTS_SCHEMA =[
    ('product_id', IntegerType()),
    ('product_name', StringType()),
    ('product_category', StringType()),
    ('updated_at', TimestampType())
]

CURRENCY_SCHEMA =[
    ('currency_value', FloatType()),
    ('currency_name', StringType()),
    ('updated_at', TimestampType())
]

GEOLOCATION_SCHEMA =[
    ('ip1', IntegerType()),
    ('ip2', IntegerType()),
    ('country_code', StringType()),
    ('country_name', StringType()),
    ('updated_at', TimestampType())
]

LOGS_SCHEMA =[
    ('time', StringType()),
    ('remote_ip', StringType()),
    ('country_name', StringType()),
    ('ip_number', IntegerType()),
    ('request', StringType()),
    ('response', StringType()),
    ('agent', StringType()),
    ('updated_at', TimestampType())
]

ECOMM_SCHEMA =[
    ('customer_name', StringType()),
    ('address', StringType()),
    ('city', StringType()),
    ('country', StringType()),
    ('currency', StringType()),
    ('email', StringType()),
    ('order_date', DateType()),
    ('order_mode', StringType()),
    ('order_number', IntegerType()),
    ('phone', StringType()),
    ('postalcode', StringType()),
    ('product_name', StringType()),
    ('sale_price', FloatType()),
    ('sale_price_usd', FloatType()),
    ('updated_at', TimestampType())
]

# COMMAND ----------

# DBTITLE 1,Define functions
def gen_blank_df(spark, schema_struct):
    fields = [StructField(*field) for field in schema_struct]
    schema = StructType(fields)
    df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    return df

def mask_value(column):
  mask_value = hashlib.sha256(column.encode()).hexdigest()
  return mask_value

def curate_email(email):
  curated_value = email.lower()
  return curated_value

def curate_country(country):
  if (country == 'USA' or country == 'United States'):
    curated_value = 'USA'
  elif (country == 'UK' or country == 'United Kingdom'):
    curated_value = 'UK'
  elif (country == 'CAN' or country == 'Canada'):
    curated_value = 'CAN'
  elif (country == 'IND' or country == 'India'):
    curated_value = 'IND'
  else:
    curated_value = country
  return curated_value

def curate_sales_price(currency, currency_value, sales_price):
  if (currency != 'USD'):
    curated_value = float(sales_price)/float(currency_value)
    return float(curated_value)
  else:
    return float(sales_price)

def ip_to_country(ip):
  ipsplit = ip.split(".")
  ip_number=16777216*int(ipsplit[0]) + 65536*int(ipsplit[1]) + 256*int(ipsplit[2]) + int(ipsplit[3])  
  return ip_number

mask_udf = udf(mask_value, StringType())
curate_email_udf = udf(curate_email, StringType())
curate_country_udf = udf(curate_country, StringType())
curate_sales_price_udf = udf(curate_sales_price, FloatType())
ip_to_country_udf = udf(ip_to_country, StringType())

# COMMAND ----------

# DBTITLE 1,Curate Currency Data - standardize, mask and merge 
currency_path="wasbs://"+SILVER_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+CURRENCY_FOLDER
fields = [StructField(*field) for field in CURRENCY_SCHEMA]
schema_currency = StructType(fields)
try:
  deltaTable = DeltaTable.forPath(spark, currency_path)
except:
  spark.sql("DROP TABLE IF EXISTS "+ CURRENCY_FOLDER)
  df_currency = gen_blank_df(spark, CURRENCY_SCHEMA)
  df_currency.write.format("delta").option("path", currency_path).saveAsTable(CURRENCY_FOLDER)
  deltaTable = DeltaTable.forPath(spark, currency_path)
    
for currency in CURRENCY_LIST.split(","):
  bronze_currency_path="wasbs://"+BRONZE_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+CURRENCY_FOLDER+"/"+currency+"/"+ADLS_FOLDER
  #print(bronze_currency_path)

  try:
    df_currency_incremental = spark.read.csv(bronze_currency_path, schema=schema_currency )
    df_currency_incremental=df_currency_incremental.withColumn('currency_name', f.lit(currency))
    df_currency_incremental=df_currency_incremental.withColumn('updated_at', f.lit(UPDATED))

    deltaTable.alias("currency").merge(
    df_currency_incremental.alias("currency_new"),
                    "currency.currency_name = currency_new.currency_name") \
                    .whenMatchedUpdate(set = {"currency_value":   "currency_new.currency_value", 	\
                                              "updated_at":       "currency_new.updated_at" } )     \
                    .whenNotMatchedInsert(values =                                                  \
                       {                                                    
                                              "currency_value":   "currency_new.currency_value", 	\
                                              "currency_name":    "currency_new.currency_name",     \
                                              "updated_at":       "currency_new.updated_at"         \
                       }                                                                            \
                     ).execute()
  except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Sales Data - standardize, mask and merge 
for table in TABLE_LIST.split(","):
  
  try:
    table_path="wasbs://"+SILVER_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+STORE_SALES_FOLDER+"/"+table
    bronze_table_path="wasbs://"+BRONZE_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+STORE_SALES_FOLDER+"/\[dbo\].\["+table+"\]/"+ADLS_FOLDER
    #print(bronze_table_path)
    #print(table_path)
  
    if table=="store_customers":
      TABLE_SCHEMA=CUSTOMERS_SCHEMA
    elif table=="store_orders":
      TABLE_SCHEMA=ORDERS_SCHEMA
    elif table=="products":
      TABLE_SCHEMA=PRODUCTS_SCHEMA
    
    
    fields = [StructField(*field) for field in TABLE_SCHEMA]
    schema_stores = StructType(fields)
    try:
      deltaTable = DeltaTable.forPath(spark, table_path)
    except:
      spark.sql("DROP TABLE IF EXISTS "+ table)
      df = gen_blank_df(spark,TABLE_SCHEMA)
      df.write.format("delta").option("path", table_path).saveAsTable(table)
      deltaTable = DeltaTable.forPath(spark, table_path)

    if table=="store_customers":
      df_table_incremental = spark.read.csv(bronze_table_path, schema=schema_stores )
      #display(df_table_incremental)
      df_table_curated = df_table_incremental.withColumn('email_curated',curate_email_udf('email')).drop('email').withColumnRenamed('email_curated', 'email')

      df_table_curated = df_table_curated.withColumn('country_curated',curate_country_udf('country')).drop('country').withColumnRenamed('country_curated', 'country')
 
      df_table_curated = df_table_curated.withColumn('phone_masked',mask_udf('phone')).drop('phone').withColumnRenamed('phone_masked', 'phone')
      df_table_curated = df_table_curated.withColumn('credit_card_masked',mask_udf('credit_card')).drop('credit_card').withColumnRenamed('credit_card_masked', 'credit_card')
      df_table_curated = df_table_curated.withColumn('credit_card_masked',mask_udf('credit_card')).drop('credit_card').withColumnRenamed('credit_card_masked', 'credit_card')
      df_table_curated = df_table_curated.withColumn('address_masked',mask_udf('address')).drop('address').withColumnRenamed('address_masked', 'address')
      df_table_curated=df_table_curated.withColumn('updated_at', f.lit(UPDATED))

      deltaTable.alias("store_customers").merge(
      df_table_curated.alias("store_customers_new"),
                      "store_customers.email = store_customers_new.email") \
                      .whenMatchedUpdate(set = {"customer_id": 	    "store_customers_new.customer_id", 	  \
                                                "customer_name":    "store_customers_new.customer_name",  \
                                                "address":          "store_customers_new.address",        \
                                                "city":             "store_customers_new.city",           \
                                                "postalcode":       "store_customers_new.postalcode",     \
                                                "country":          "store_customers_new.country",        \
                                                "phone":            "store_customers_new.phone",          \
                                                "email":            "store_customers_new.email",          \
                                                "credit_card":      "store_customers_new.credit_card",    \
                                                "updated_at":       "store_customers_new.updated_at" } )  \
                      .whenNotMatchedInsert(values =                                                      \
                         {                                                    
                                                "customer_id": 	    "store_customers_new.customer_id", 	  \
                                                "customer_name":    "store_customers_new.customer_name",  \
                                                "address":          "store_customers_new.address",        \
                                                "city":             "store_customers_new.city",           \
                                                "postalcode":       "store_customers_new.postalcode",     \
                                                "country":          "store_customers_new.country",        \
                                                "phone":            "store_customers_new.phone",          \
                                                "email":            "store_customers_new.email",          \
                                                "credit_card":      "store_customers_new.credit_card",    \
                                                "updated_at":       "store_customers_new.updated_at"      \
                         }                                                                                \
                       ).execute()
    elif table=="store_orders":
      ORDERS_SCHEMA_1 =[('order_number', IntegerType()),('customer_id', IntegerType()),('product_id', IntegerType()),('order_date', StringType()),
                        ('units', IntegerType()),('sale_price', FloatType()), ('currency', StringType()), 
                        ('order_mode', StringType()), ('sale_price_usd', FloatType()), ('updated_at', TimestampType())
                       ]
      fields = [StructField(*field) for field in ORDERS_SCHEMA_1]
      schema_stores = StructType(fields)
      df_table_incremental = spark.read.csv(bronze_table_path, schema=schema_stores )

      df_currency=spark.sql('SELECT currency_name AS currency, currency_value from currency')
      columns = ['currency', 'currency_value']
      df_currency_usd = spark.createDataFrame([('USD','1')], columns)
      df_currency_final=df_currency_usd.union(df_currency)
     
      df_table_curated = df_table_incremental.join(df_currency_final, on=['currency'], how="inner")
      df_table_curated = df_table_curated.withColumn('sale_price_usd',curate_sales_price_udf('currency', 'currency_value', 'sale_price'))
      df_table_curated=df_table_curated.withColumn('updated_at', f.lit(UPDATED))
      df_table_curated = df_table_curated.withColumn('order_date_new', to_date(df_table_curated.order_date, 'MM/dd/yyyy')).drop('order_date').withColumnRenamed('order_date_new', 'order_date')
      df_table_curated = df_table_curated.drop('currency_value')


      deltaTable.alias("store_orders").merge(
      df_table_curated.alias("store_orders_new"),
                      "store_orders.order_number = store_orders_new.order_number")                     \
                      .whenMatchedUpdate(set = {"order_number": 	"store_orders_new.order_number",   \
                                                "customer_id":      "store_orders_new.customer_id",    \
                                                "product_id":       "store_orders_new.product_id",     \
                                                "order_date":       "store_orders_new.order_date",     \
                                                "units":            "store_orders_new.units",          \
                                                "sale_price":       "store_orders_new.sale_price",     \
                                                "sale_price_usd":   "store_orders_new.sale_price_usd", \
                                                "currency":         "store_orders_new.currency",       \
                                                "order_mode":       "store_orders_new.order_mode",     \
                                                "updated_at":       "store_orders_new.updated_at" } )  \
                      .whenNotMatchedInsert(values =                                                   \
                         {                                                    
                                                "order_number": 	"store_orders_new.order_number",   \
                                                "customer_id":      "store_orders_new.customer_id",    \
                                                "product_id":       "store_orders_new.product_id",     \
                                                "order_date":       "store_orders_new.order_date",     \
                                                "units":            "store_orders_new.units",          \
                                                "sale_price":       "store_orders_new.sale_price",     \
                                                "sale_price_usd":   "store_orders_new.sale_price_usd", \
                                                "currency":         "store_orders_new.currency",       \
                                                "order_mode":       "store_orders_new.order_mode",     \
                                                "updated_at":       "store_orders_new.updated_at"      \
                         }                                                                             \
                       ).execute()
      deltaTable.delete("order_mode = 'DELETE'")
    elif table=="products":
      df_table_incremental = spark.read.csv(bronze_table_path, schema=schema_stores )
      df_table_curated=df_table_incremental.withColumn('updated_at', f.lit(UPDATED))

      deltaTable.alias("products").merge(
      df_table_curated.alias("products_new"),
                      "products.product_id = products_new.product_id")                                \
                      .whenMatchedUpdate(set = {"product_id": 	    "products_new.product_id", 	      \
                                                "product_name":     "products_new.product_name",      \
                                                "product_category": "products_new.product_category",  \
                                                "updated_at":       "products_new.updated_at" } )     \
                      .whenNotMatchedInsert(values =                                                  \
                         {                                                    
                                                "product_id": 	    "products_new.product_id", 	      \
                                                "product_name":     "products_new.product_name",      \
                                                "product_category": "products_new.product_category",  \
                                                "updated_at":       "products_new.updated_at"         \
                         }                                                                            \
                       ).execute()  

  except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Find orphan store_orders
orphan_path="wasbs://"+SILVER_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/exceptions/orphan_orders/"+ADLS_FOLDER
df_store_orders_orphan=spark.sql("SELECT * FROM store_orders WHERE product_id NOT IN (SELECT product_id FROM products)")
df_store_orders_orphan.write.parquet(orphan_path)

# COMMAND ----------

# DBTITLE 1,Curate Geolocation Data - standardize, mask and merge 
geolocation_path="wasbs://"+SILVER_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+GEOLOCATION_FOLDER
fields = [StructField(*field) for field in GEOLOCATION_SCHEMA]
schema_geolocation = StructType(fields)
try:
  deltaTable = DeltaTable.forPath(spark, geolocation_path)
except:
  spark.sql("DROP TABLE IF EXISTS "+ GEOLOCATION_FOLDER)
  df_geolocation = gen_blank_df(spark, GEOLOCATION_SCHEMA)
  df_geolocation.write.format("delta").option("path", geolocation_path).saveAsTable(GEOLOCATION_FOLDER)
  deltaTable = DeltaTable.forPath(spark, geolocation_path)

bronze_geolocation_path="wasbs://"+BRONZE_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+GEOLOCATION_FOLDER+"/"+ADLS_FOLDER

try:
  df_geolocation_incremental = spark.read.csv(bronze_geolocation_path, schema=schema_geolocation )
  df_geolocation_incremental=df_geolocation_incremental.withColumn('updated_at', f.lit(UPDATED))

  deltaTable.alias("geolocation").merge(
    df_geolocation_incremental.alias("geolocation_new"),
                      "geolocation.ip1 = geolocation_new.ip1") \
                      .whenMatchedUpdate(set = {"ip2":              "geolocation_new.ip2", 	             \
                                                "country_code":     "geolocation_new.country_code",        \
                                                "country_name":     "geolocation_new.country_name",        \
                                                "updated_at":       "geolocation_new.updated_at" } )       \
                      .whenNotMatchedInsert(values =                                                       \
                         {                                                    
                                                "ip1":              "geolocation_new.ip1", 	             \
                                                "ip2":              "geolocation_new.ip2", 	             \
                                                "country_code":     "geolocation_new.country_code",        \
                                                "country_name":     "geolocation_new.country_name",        \
                                                "updated_at":       "geolocation_new.updated_at"           \
                         }                                                                                 \
                       ).execute()                     
except Exception as e:
  print(e)

# COMMAND ----------

# DBTITLE 1,Curate Logs Data - standardize, mask and merge 
logs_path="wasbs://"+SILVER_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+LOGS_FOLDER
fields = [StructField(*field) for field in LOGS_SCHEMA]
schema_logs = StructType(fields)
try:
  deltaTable = DeltaTable.forPath(spark, logs_path)
except:
  spark.sql("DROP TABLE IF EXISTS "+ LOGS_FOLDER)
  df_logs = gen_blank_df(spark, LOGS_SCHEMA)
  df_logs.write.format("delta").option("path", logs_path).saveAsTable(LOGS_FOLDER)
  deltaTable = DeltaTable.forPath(spark, logs_path)
  
bronze_logs_path="wasbs://"+BRONZE_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+LOGS_FOLDER+"/"+ADLS_FOLDER

try:
  df_logs_incremental = spark.read.json(bronze_logs_path, schema=schema_logs )
  df_logs_incremental = df_logs_incremental.withColumn('updated_at', f.lit(UPDATED))
  df_logs_incremental = df_logs_incremental.withColumn('time', from_unixtime(unix_timestamp('time', 'dd/MM/yyy:HH:m:ss')))
  df_logs_incremental = df_logs_incremental.withColumn('ip_number',ip_to_country_udf('remote_ip'))
  df_logs_incremental = df_logs_incremental.withColumn("ip_number_int", df_logs_incremental['ip_number'].cast('int')).drop('ip_number').withColumnRenamed('ip_number_int', 'ip_number')
  df_logs_incremental.registerTempTable('logs_incr')
  
  df_logs_incremental=spark.sql(" SELECT logs_incr.time, logs_incr.remote_ip, geolocation.country_name, logs_incr.ip_number, logs_incr.request, logs_incr.response, " \
                                " logs_incr.agent, logs_incr.updated_at " \
                                " FROM logs_incr JOIN geolocation WHERE ip1 <= ip_number AND ip2 >= ip_number")
  
  deltaTable.alias("logs").merge(
      df_logs_incremental.alias("logs_incr"),
      "logs.remote_ip = logs_incr.remote_ip") \
    .whenNotMatchedInsertAll() \
    .execute()
  
except Exception as e:
    print(e)

# COMMAND ----------

# DBTITLE 1,Curate eCommerce Sales Data - standardize, mask and merge 
ecomm_path="wasbs://"+SILVER_LAYER_NAMESPACE+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"+ECOMM_FOLDER.split("/")[0]

fields = [StructField(*field) for field in ECOMM_SCHEMA]
schema_ecomm = StructType(fields)
try:
  deltaTable = DeltaTable.forPath(spark, ecomm_path)
except:
  spark.sql("DROP TABLE IF EXISTS "+ ECOMM_FOLDER.split("/")[0])
  df_logs = gen_blank_df(spark, ECOMM_SCHEMA)
  df_logs.write.format("delta").option("path", ecomm_path).saveAsTable(ECOMM_FOLDER.split("/")[0])
  deltaTable = DeltaTable.forPath(spark, ecomm_path)

bronze_ecomm_path="wasbs://bronze@traininglakehouse.blob.core.windows.net/"+ECOMM_FOLDER

try:
  df_ecomm=spark.read.format("avro").load(bronze_ecomm_path)

  df_ecomm_json = df_ecomm.select(df_ecomm.Body.cast("string")).rdd.map(lambda x: x[0])
  df_ecomm_data = spark.read.json(df_ecomm_json).select('data')
  df_data_values = df_ecomm_data.select('data.customer_name', 'data.address', 'data.city', 'data.country', 'data.currency', 'data.email',
                                        'data.order_date', 'data.order_mode', 'data.order_number', 'data.phone','data.postalcode', 'data.product_name', 'data.sale_price' )

  df_data_values = df_data_values.withColumn('updated_at', f.lit(UPDATED))
  df_data_values = df_data_values.withColumn('phone_masked',mask_udf('phone')).drop('phone').withColumnRenamed('phone_masked', 'phone')
  df_data_values = df_data_values.withColumn('address_masked',mask_udf('address')).drop('address').withColumnRenamed('address_masked', 'address')
  df_data_values = df_data_values.withColumn('order_date', from_unixtime(unix_timestamp('order_date', 'dd/MM/yyy')))
  df_data_values = df_data_values.withColumn('country_curated',curate_country_udf('country')).drop('country').withColumnRenamed('country_curated', 'country')
  
  df_data_values = df_data_values.join(df_currency_final, on=['currency'], how="inner")
  df_data_values = df_data_values.withColumn('sale_price_usd',curate_sales_price_udf('currency', 'currency_value', 'sale_price'))

  df_data_values = df_data_values.withColumn('order_date_new', to_date(df_data_values.order_date, 'yyyy-MM-dd HH:mm:ss')).drop('order_date').withColumnRenamed('order_date_new', 'order_date')
  
  #display(df_data_values)
  deltaTable.alias("esalesns").merge(
  df_data_values.alias("esalesns_new"),
                    "esalesns.email = esalesns_new.email")                                          \
                    .whenMatchedUpdate(set = {"customer_name":    "esalesns_new.customer_name", 	\
                                              "address":          "esalesns_new.address",           \
                                              "city":             "esalesns_new.city",              \
                                              "country":          "esalesns_new.country",           \
                                              "currency":         "esalesns_new.currency",          \
                                              "email":            "esalesns_new.email",             \
                                              "order_date":       "esalesns_new.order_date",        \
                                              "order_mode":       "esalesns_new.order_mode",        \
                                              "order_number":     "esalesns_new.order_number",      \
                                              "phone":            "esalesns_new.phone",             \
                                              "postalcode":       "esalesns_new.postalcode",        \
                                              "product_name":     "esalesns_new.product_name",      \
                                              "sale_price":       "esalesns_new.sale_price",        \
                                              "sale_price_usd":   "esalesns_new.sale_price_usd",    \
                                              "updated_at":       "esalesns_new.updated_at" } )     \
                    .whenNotMatchedInsert(values =                                                  \
                       {                                         
                                              "customer_name":    "esalesns_new.customer_name", 	\
                                              "address":          "esalesns_new.address",           \
                                              "city":             "esalesns_new.city",              \
                                              "country":          "esalesns_new.country",           \
                                              "currency":         "esalesns_new.currency",          \
                                              "email":            "esalesns_new.email",             \
                                              "order_date":       "esalesns_new.order_date",        \
                                              "order_mode":       "esalesns_new.order_mode",        \
                                              "order_number":     "esalesns_new.order_number",      \
                                              "phone":            "esalesns_new.phone",             \
                                              "postalcode":       "esalesns_new.postalcode",        \
                                              "product_name":     "esalesns_new.product_name",      \
                                              "sale_price":       "esalesns_new.sale_price",        \
                                              "sale_price_usd":   "esalesns_new.sale_price_usd",    \
                                              "updated_at":       "esalesns_new.updated_at"         \
                       }                                                                            \
                     ).execute()
except Exception as e:
  print(e)
