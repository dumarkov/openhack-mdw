# Databricks notebook source
src_mount_point = '/mnt/dumarkovopenhackmdw/source-data'
dst_mount_point = '/mnt/dumarkovopenhackmdw/databases'

paths = [
   'fourthcoffee/rentals'
  ,'vanarsdelltd/onpremrentals/dbo'
  ,'southridge/southridge'
  ,'southridge/cloudsales/dbo'
  ,'southridge/cloudstreaming/dbo'
]

# COMMAND ----------

# DBTITLE 1,Drop external tables databases
stmt= "drop database if exists {db_name} cascade"

for p in paths:
  db_name = p.replace('/','_') + '_x'
  sql(eval(f'f"{stmt}"'))

# COMMAND ----------

# DBTITLE 1,Create external tables databases
stmt= "create database if not exists {db_name} location '{dst_mount_point}/dummy/{db_name}'"

for p in paths:
  db_name = p.replace('/','_') + '_x'
  sql(eval(f'f"{stmt}"'))

# COMMAND ----------

# DBTITLE 1,Create external tables for all folders except fourthcoffee/rentals
stmt = """
create table if not exists {db_name}.{table_name} 
using parquet 
location '{path}'
"""

for p in paths[1:]: # skip 'fourthcoffee/rentals'
  db_name = p.replace('/','_') + '_x'
  dir_path = f'{src_mount_point}/{p}'
  
  for f in dbutils.fs.ls(dir_path):
    path = f.path
    table_name = (f.name).split('.')[0]
    sql(eval(f'f"""{stmt}"""'))

# COMMAND ----------

# DBTITLE 1,Create external tables for fourthcoffee/rentals based on external tables in vanarsdelltd_onpremrentals_dbo_x
adjustments = {'transactions': {'RewindFlag': 'tinyint'}}

stmt = """
create table if not exists {db_name}.{table_name} 
({schema})
using csv 
options (header = True)
location '{path}'
"""

path = 'fourthcoffee/rentals'
db_name = path.replace('/','_') + '_x'
dir_path = f'{src_mount_point}/{path}'

for f in dbutils.fs.ls(dir_path):
  
  path = f.path
  table_name = f.name.split('.')[0].lower()

  df = spark.table(f'vanarsdelltd_onpremrentals_dbo_x.{table_name}')
  schema = ','.join(['{} {}'.format(c['name'], adjustments.get(table_name,{}).get(c['name'],c['type'])) for c in df.schema.jsonValue()['fields']])

  sql(eval(f'f"""{stmt}"""'))

# COMMAND ----------

for d in sql("show databases like '*_x'").collect():
  print('-'*20 + d.databaseName)
  for t in sql(f"show tables in {d.databaseName}").collect():
    print(t.tableName)

# COMMAND ----------

rows_per_table = 10

cell_sep = '\n\n' '-- COMMAND ----------' '\n'

for d in sql("show databases like '*_x'").collect():
  print(f'%md #{d.databaseName}{cell_sep}')
  for t in sql(f"show tables in {d.databaseName}").collect():
    print(f'-- DBTITLE 1,{t.tableName}')
    print(f'select * from {d.databaseName}.{t.tableName} limit {rows_per_table}{cell_sep}')