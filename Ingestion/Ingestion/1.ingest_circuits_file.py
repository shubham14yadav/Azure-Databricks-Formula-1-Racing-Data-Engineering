# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read the CSV file using the spark dataframe reader

# COMMAND ----------

circuits_df=spark.read\
.option("header",True) \
.option("InferSchema",True) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

#Access Azure Data Lake using Access Keys
display(dbutils.fs.ls("abfss://raw@formula1dlshu.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#%fs
#ls /mnt/formula1dlshu/raw

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

from pyspark.sql.types import IntegerType

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(),False),
                                    StructField("circuitRef",StringType(),True),
                                    StructField("name",StringType(),True),
                                    StructField("location",StringType(),True),
                                    StructField("country",StringType(),True),
                                    StructField("lat",DoubleType(),True),
                                    StructField("lng",DoubleType(),True),
                                    StructField("alt",IntegerType(),True),
                                    StructField("url",StringType(),True)
])

# COMMAND ----------

circuits_df=spark.read\
.option("header",True) \
.schema(circuits_schema) \
.csv("abfss://raw@formula1dlshu.dfs.core.windows.net/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_selected_df=circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Rename the column as Required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitId","circuit_id")\
.withColumnRenamed("circuitRef","circuit_ref")\
.withColumnRenamed("lat","latitude")\
.withColumnRenamed("lng","longitude")\
.withColumnRenamed("alt","altitude") \
.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp())

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - Write data to datalake as parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ####
# MAGIC circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

display(spark.read.parquet("abfss://processed@formula1dlshu.dfs.core.windows.net/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")