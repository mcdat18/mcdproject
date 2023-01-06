# Databricks notebook source
# DBTITLE 1,Test pipeline
# dbutils.notebook.run(path: String,  timeout_seconds: int, arguments: Map): String

# Set the date param
# Remember we only have files from 2022-09-10, 2022-09-11, 2022-09-12
p_file_date = "2022-09-10"

# If you remember at the end of our python files in the ingestion folder we set a dbutils.notebook.exit("Success")
# We are going to save that value on our v_result variable and display it
# If Success message is displayed that means our notebook runs successfully
v_result = dbutils.notebook.run('../ingestion/customer', 0, {"p_file_date": p_file_date})
v_result
Test

v_result = dbutils.notebook.run('../ingestion/customerDrivers', 0, {"p_file_date": p_file_date})
v_result


v_result = dbutils.notebook.run('../ingestion/loanTransaction', 0, {"p_file_date": p_file_date})
v_result

# Uncomment this lines when you get to Part 6 (Enrichment)
# Enrichment customer doesn't need the file date param. 
v_result = dbutils.notebook.run('../enrichment/customer', 0)
v_result

# Uncomment this lines when you get to Part 6 (Enrichment)
v_result = dbutils.notebook.run('../enrichment/loanTrx', 0, {"p_file_date": p_file_date})
v_result
