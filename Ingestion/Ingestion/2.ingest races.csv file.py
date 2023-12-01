# Databricks notebook source
# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(),True)                                                                  
])

# COMMAND ----------

races_df=spark.read \
.option("header",True)\
.schema(races_schema)\
.csv("abfss://raw@formula1dlshu.dfs.core.windows.net/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit, to_timestamp


# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date",current_timestamp())\
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Select only the columns required & rename as required

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col('raceId'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC races_selected_df.write.mode('overwrite').parquet("abfss://processed@formula1dlshu.dfs.core.windows.net/races")

# COMMAND ----------

races_selected_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %md
# MAGIC races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet("abfss://processed@formula1dlshu.dfs.core.windows.net/races")

# COMMAND ----------

# MAGIC %md
# MAGIC display(spark.read.parquet("abfss://processed@formula1dlshu.dfs.core.windows.net/races"))

# COMMAND ----------

