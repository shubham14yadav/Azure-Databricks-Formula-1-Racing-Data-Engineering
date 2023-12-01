# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying JSON files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 1 Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DateType, FloatType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                 StructField("raceId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("constructorId",IntegerType(),True),
                                 StructField("number", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("q1", StringType(), True),
                                 StructField("q2", StringType(), True),
                                 StructField("q3", StringType(), True)
                                                                                      
                                ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json("abfss://raw@formula1dlshu.dfs.core.windows.net/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to processed container in parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC final_df.write.mode("overwrite").parquet("abfss://processed@formula1dlshu.dfs.core.windows.net/qualifying")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")