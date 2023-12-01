-- Databricks notebook source
show databases;

-- COMMAND ----------

show tables;

-- COMMAND ----------

use f1_processed;

-- COMMAND ----------

select * from drivers;

-- COMMAND ----------

select name,dob as date_of_birth
from drivers
where nationality = 'British'
and dob >='1990-01-01'
order by dob;