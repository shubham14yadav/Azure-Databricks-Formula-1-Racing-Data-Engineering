-- Databricks notebook source
show databases;

-- COMMAND ----------

use f1_presentation;

-- COMMAND ----------

desc driver_standings;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW V_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year =2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW V_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
from driver_standings
where race_year =2020;

-- COMMAND ----------

select * 
from V_driver_standings_2018

-- COMMAND ----------

select *
from V_driver_standings_2018 d_2018
semi join V_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC anti join - in left but not in right

-- COMMAND ----------

select *
from V_driver_standings_2018 d_2018
anti join V_driver_standings_2020 d_2020
on (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC everything from left to right and cartesian product too

-- COMMAND ----------

select *
from V_driver_standings_2018 d_2018
cross join V_driver_standings_2020 d_2020