-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC v_cols = \
-- MAGIC [(d.databaseName, t.tableName, c.col_name, c.data_type) 
-- MAGIC  for d in sql("show databases like '*_v'").collect() 
-- MAGIC  for t in sql(f"show tables in {d.databaseName}").collect() if t.database != ''
-- MAGIC  for c in sql(f"desc {d.databaseName}.{t.tableName}").collect()]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df = spark.createDataFrame(v_cols, ['db','tab','col','typ'])
-- MAGIC df.createOrReplaceTempView('v_cols')

-- COMMAND ----------

with 

cols as 
(
select   tab, col ,count(distinct db) as col_dbs_cnt, array_sort(collect_list(db)) as col_dbs
from      v_cols
group by  tab, col
),

tabs as 
(
select    tab, count(distinct db) as tab_dbs_cnt, array_sort(collect_set(db)) as tab_dbs           
from      v_cols   
group by  tab
),

tabs_cols as 
(
select    c.* , t.tab_dbs, t.tab_dbs_cnt
from      cols c join tabs t on t.tab = c.tab
)

select    tab
         ,min(tab_dbs_cnt) as tab_dbs_cnt
         ,min(tab_dbs)     as dbs
         ,array_sort(collect_set(case when col_dbs_cnt = tab_dbs_cnt then col end)) as cols_common 
         ,array_sort(collect_set(case when col_dbs_cnt < tab_dbs_cnt then col end)) as cols_diff 
         
from      tabs_cols

group by  tab

-- COMMAND ----------

with 

cols as 
(
select   tab, col ,count(distinct db) as col_dbs_cnt, array_sort(collect_list(db)) as col_dbs
from      v_cols
group by  tab, col
),

tabs as 
(
select    tab, count(distinct db) as tab_dbs_cnt, array_sort(collect_set(db)) as tab_dbs           
from      v_cols   
group by  tab
),

tabs_cols as 
(
select    c.* , t.tab_dbs, t.tab_dbs_cnt
from      cols c join tabs t on t.tab = c.tab
)

select    tab
         ,min(tab_dbs_cnt) as tab_dbs_cnt
         ,min(tab_dbs)     as dbs
         ,array_sort(collect_set(case when col_dbs_cnt = tab_dbs_cnt then col end))                  as cols_common 
         ,map_from_entries(collect_set(case when col_dbs_cnt < tab_dbs_cnt then (col, col_dbs) end)) as cols_diff 
         
from      tabs_cols

group by  tab

-- COMMAND ----------

select    col
         ,count(*) as cnt
         ,collect_list(concat_ws('.',db,tab))
         
from      v_cols

group by  col

order by  col


-- COMMAND ----------

select    typ
         ,count(distinct col)          as cols_cnt_distinct
         ,array_sort(collect_set(col)) as cols
         
from      v_cols

group by  typ

order by  typ

-- COMMAND ----------

select    col
         ,count(distinct typ)          as typ_cnt
         ,collect_set(typ)             as typs
         ,collect_list((db, tab, typ)) as tabs
         
from      v_cols

group by  col

having    typ_cnt > 1