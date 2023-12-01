# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Access Keys
# MAGIC 1. Set spark config for Access Keys
# MAGIC 1. List files from Demo container
# MAGIC 1. Read data from circuit.csv file

# COMMAND ----------

dbutils.secrets.list(scope="formula-scope")

# COMMAND ----------

dbutils.secrets.get(scope='formula-scope', key='formula1dlsh-account-key')

# COMMAND ----------

formula1dlsh_account_key = dbutils.secrets.get(scope='formula-scope', key='formula1dlsh-account-key')
spark.conf.set("fs.azure.account.key.formula1dlshu.dfs.core.windows.net", formula1dlsh_account_key)


# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlshu.dfs.core.windows.net", formula1dlsh_account_key)

# COMMAND ----------

#Access Azure Data Lake using Access Keys
display(dbutils.fs.ls("abfss://demo@formula1dlshu.dfs.core.windows.net"))

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formula1dlshu.dfs.core.windows.net","9te7kev8AWHE8FF8O3eXJtpkZjblefCRA5hHgXdteM7IRDFuIQcSBM8D7bqi4N5tfnjTJ3GVkfxE+AStcxJdZw==")

# COMMAND ----------

#Access Azure Data Lake using Access Keys
display(dbutils.fs.ls("abfss://demo@formula1dlshu.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlshu.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

