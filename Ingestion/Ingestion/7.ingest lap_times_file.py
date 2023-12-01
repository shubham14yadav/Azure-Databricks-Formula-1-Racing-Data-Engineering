# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step - 1 Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType, DateType, FloatType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("time", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True)                                                      
                                ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("abfss://raw@formula1dlshu.dfs.core.windows.net/lap_times/lap_times_split*.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to processed container in parquet file

# COMMAND ----------

# MAGIC %md
# MAGIC final_df.write.mode("overwrite").parquet("abfss://processed@formula1dlshu.dfs.core.windows.net/lap_times")

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")