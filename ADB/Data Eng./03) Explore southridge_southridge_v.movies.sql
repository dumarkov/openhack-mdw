-- Databricks notebook source
select * from southridge_southridge_x.movies

-- COMMAND ----------

-- MAGIC %python print('\n\t\t,'.join(spark.table('southridge_southridge_x.movies').columns))

-- COMMAND ----------

select actors
from   southridge_southridge_x.movies
limit  5

-- COMMAND ----------

select schema_of_json('[{"name":"Eric Ray"},{"name":"Danielle Busey"},{"name":"Priscilla Wayne"}]')

-- COMMAND ----------

select   from_json(actors, 'array<struct<name:string>>')

from     southridge_southridge_x.movies

limit    10

-- COMMAND ----------

select   id
        ,inline(from_json(actors, 'array<struct<name:string>>')) as name

from     southridge_southridge_x.movies