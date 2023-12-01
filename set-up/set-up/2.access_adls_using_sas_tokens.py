# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set spark config for Access Keys
# MAGIC 1. List files from Demo container
# MAGIC 1. Read data from circuit.csv file

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlshu.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlshu.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlshu.dfs.core.windows.net","sp=rl&st=2023-05-19T23:12:06Z&se=2023-05-20T07:12:06Z&spr=https&sv=2022-11-02&sr=c&sig=VFYO5nPII1tg4aniMGyIbaHu2AOv%2BJG2WQlKIftc%2FHo%3D")

# COMMAND ----------

#Access Azure Data Lake using SAS tokens
display(dbutils.fs.ls("abfss://demo@formula1dlshu.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlshu.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

