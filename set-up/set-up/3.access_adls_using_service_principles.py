# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principles
# MAGIC 1. Set spark config for Service principles
# MAGIC 1. List files from Demo container
# MAGIC 1. Read data from circuit.csv file

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula-scope', key = 'formula1-app-client-secret')


# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlshu.dfs.core.windows.net","9te7kev8AWHE8FF8O3eXJtpkZjblefCRA5hHgXdteM7IRDFuIQcSBM8D7bqi4N5tfnjTJ3GVkfxE+AStcxJdZw==")

# COMMAND ----------

#Access Azure Data Lake using Access Keys
display(dbutils.fs.ls("abfss://demo@formula1dlshu.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlshu.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

