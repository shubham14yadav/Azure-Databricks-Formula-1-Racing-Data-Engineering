# Databricks notebook source
# MAGIC %md
# MAGIC #### Step - 1 Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df=spark.read \
.schema(constructors_schema) \
.json("abfss://raw@formula1dlshu.dfs.core.windows.net/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

constructors_dropped_df=constructors_df.drop(constructors_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
.withColumnRenamed("constructorRef", "constructor_ref") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC constructors_final_df.write.mode("overwrite").parquet("abfss://processed@formula1dlshu.dfs.core.windows.net/constructors")

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

