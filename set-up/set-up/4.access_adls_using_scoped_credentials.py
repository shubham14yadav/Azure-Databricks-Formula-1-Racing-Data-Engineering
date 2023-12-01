# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Scoped Credentials
# MAGIC 1. Set spark config for e Keys
# MAGIC 1. List files from Demo container
# MAGIC 1. Read data from circuit.csv file

# COMMAND ----------

#Access Azure Data Lake using Access Keys
display(dbutils.fs.ls("abfss://demo@formula1dlshu.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlshu.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

