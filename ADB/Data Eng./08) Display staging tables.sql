-- Databricks notebook source
-- MAGIC %md #fourthcoffee_rentals_stg

-- COMMAND ----------

-- DBTITLE 1,actors
select * from fourthcoffee_rentals_stg.actors limit 10

-- COMMAND ----------

-- DBTITLE 1,addresses
select * from fourthcoffee_rentals_stg.addresses limit 10

-- COMMAND ----------

-- DBTITLE 1,customers
select * from fourthcoffee_rentals_stg.customers limit 10

-- COMMAND ----------

-- DBTITLE 1,movieactors
select * from fourthcoffee_rentals_stg.movieactors limit 10

-- COMMAND ----------

-- DBTITLE 1,movies
select * from fourthcoffee_rentals_stg.movies limit 10

-- COMMAND ----------

-- DBTITLE 1,onlinemoviemappings
select * from fourthcoffee_rentals_stg.onlinemoviemappings limit 10

-- COMMAND ----------

-- DBTITLE 1,transactions
select * from fourthcoffee_rentals_stg.transactions limit 10

-- COMMAND ----------

-- MAGIC %md #southridge_cloudsales_dbo_stg

-- COMMAND ----------

-- DBTITLE 1,addresses
select * from southridge_cloudsales_dbo_stg.addresses limit 10

-- COMMAND ----------

-- DBTITLE 1,customers
select * from southridge_cloudsales_dbo_stg.customers limit 10

-- COMMAND ----------

-- DBTITLE 1,orderdetails
select * from southridge_cloudsales_dbo_stg.orderdetails limit 10

-- COMMAND ----------

-- DBTITLE 1,orders
select * from southridge_cloudsales_dbo_stg.orders limit 10

-- COMMAND ----------

-- MAGIC %md #southridge_cloudstreaming_dbo_stg

-- COMMAND ----------

-- DBTITLE 1,addresses
select * from southridge_cloudstreaming_dbo_stg.addresses limit 10

-- COMMAND ----------

-- DBTITLE 1,customers
select * from southridge_cloudstreaming_dbo_stg.customers limit 10

-- COMMAND ----------

-- DBTITLE 1,transactions
select * from southridge_cloudstreaming_dbo_stg.transactions limit 10

-- COMMAND ----------

-- MAGIC %md #southridge_southridge_stg

-- COMMAND ----------

-- DBTITLE 1,actors
select * from southridge_southridge_stg.actors limit 10

-- COMMAND ----------

-- DBTITLE 1,movies
select * from southridge_southridge_stg.movies limit 10

-- COMMAND ----------

-- MAGIC %md #vanarsdelltd_onpremrentals_dbo_stg

-- COMMAND ----------

-- DBTITLE 1,actors
select * from vanarsdelltd_onpremrentals_dbo_stg.actors limit 10

-- COMMAND ----------

-- DBTITLE 1,addresses
select * from vanarsdelltd_onpremrentals_dbo_stg.addresses limit 10

-- COMMAND ----------

-- DBTITLE 1,customers
select * from vanarsdelltd_onpremrentals_dbo_stg.customers limit 10

-- COMMAND ----------

-- DBTITLE 1,movieactors
select * from vanarsdelltd_onpremrentals_dbo_stg.movieactors limit 10

-- COMMAND ----------

-- DBTITLE 1,movies
select * from vanarsdelltd_onpremrentals_dbo_stg.movies limit 10

-- COMMAND ----------

-- DBTITLE 1,onlinemoviemappings
select * from vanarsdelltd_onpremrentals_dbo_stg.onlinemoviemappings limit 10

-- COMMAND ----------

-- DBTITLE 1,transactions
select * from vanarsdelltd_onpremrentals_dbo_stg.transactions limit 10