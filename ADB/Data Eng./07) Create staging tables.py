# Databricks notebook source
mount_point = '/mnt/dumarkovopenhackmdw/databases'

paths = [
   'fourthcoffee/rentals'
  ,'vanarsdelltd/onpremrentals/dbo'
  ,'southridge/southridge'
  ,'southridge/cloudsales/dbo'
  ,'southridge/cloudstreaming/dbo'
]

# COMMAND ----------

stmt= "drop database if exists {db_name} cascade"

for p in paths:
  db_name = p.replace('/','_') + '_stg'
  sql(eval(f'f"{stmt}"'))

# COMMAND ----------

stmt= "create database if not exists {db_name} location '{mount_point}/stg/{db_name}'"

for p in paths:
  db_name = p.replace('/','_') + '_stg'
  sql(eval(f'f"{stmt}"'))

# COMMAND ----------

stmt = '''
create table {dst_db_name}.{table_name}
using delta
as
select *
from   {src_db_name}.{table_name}
'''


for d in sql("show databases like '*_v'").collect():
  
  src_db_name = d.databaseName
  base_db_name = d.databaseName[:-2]
  dst_db_name = base_db_name + '_stg'
  
  for t in sql(f"show tables in {src_db_name}").collect():
    
    table_name = t.tableName
    sql(eval(f"f'''{stmt}'''"))

# COMMAND ----------

for d in sql("show databases like '*_stg'").collect():
  print('-'*20 + d.databaseName)
  for t in sql(f"show tables in {d.databaseName}").collect():
    print(t.tableName)

# COMMAND ----------

rows_per_table = 10

cell_sep = '\n\n' '-- COMMAND ----------' '\n'

for d in sql("show databases like '*_stg'").collect():
  print(f'%md #{d.databaseName}{cell_sep}')
  for t in sql(f"show tables in {d.databaseName}").collect():
    print(f'-- DBTITLE 1,{t.tableName}')
    print(f'select * from {d.databaseName}.{t.tableName} limit {rows_per_table}{cell_sep}')