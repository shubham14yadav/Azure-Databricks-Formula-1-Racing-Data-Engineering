-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objectives
-- MAGIC ##### 1. Spark SQL documentation
-- MAGIC ##### 2. Create Database demo
-- MAGIC ##### 3. Data tab in the UI
-- MAGIC ##### 4. SHOW command
-- MAGIC ##### 5. DESCRIBE command
-- MAGIC ##### 6. FInd the current database
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create managed table using python
-- MAGIC 2. create managed table using sql
-- MAGIC 3. effect of dropping a managed table
-- MAGIC 4. describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

select *
from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

create table race_results_sql
as
select *
from demo.race_results_python
where race_year = 2020;

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

drop table race_results_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Learning Objectives
-- MAGIC 1. create external table using python
-- MAGIC 2. create external table using sql
-- MAGIC 3. effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended race_results_ext_py

-- COMMAND ----------

create table demo.race_results_ext_sql
(
  race_year INT,
  race_name STRING,
  race_data timestamp,
  circuit_location string,
  driver_name string,
  driver_number int,
  driver_nationality string,
  team string,
  grid int,
  fastest_lap int,
  race_time string,
  points float,
  position int,
  created_date timestamp
)
using parquet
location "abfss://presentation@formula1dlshu.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
select * from demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

select count(*) from race_results_ext_sql;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Views on tables
-- MAGIC ##### Learning Objectives
-- MAGIC 1. create temp view
-- MAGIC 2. create global temp view
-- MAGIC 3. create permanent view

-- COMMAND ----------

create or replace temp view v_race_results
as
select *
  from demo.race_results_python
  where race_year = 2018;

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

create or replace global temp view gv_race_results
as
select *
  from demo.race_results_python
  where race_year = 2018;

-- COMMAND ----------

select * from global_temp.gv_race_results;

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######accessible even after detaching from cluster

-- COMMAND ----------

create or replace view demo.pv_race_results
as
select *
  from demo.race_results_python
  where race_year = 2018;

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select * from demo.pv_race_results;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

