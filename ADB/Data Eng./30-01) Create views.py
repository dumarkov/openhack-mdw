# Databricks notebook source
databases_mount_point = '/mnt/dumarkovopenhackmdw/databases'

# COMMAND ----------

# DBTITLE 1,Drop & create views databases
drop_stmt = "drop database if exists {db_name} cascade"
create_stmt = "create database if not exists {db_name} location '{databases_mount_point}/dummy/{db_name}'"

for s in spark.table('process_mng.src').collect():
  db_name = f'{s.src_name}_v'
  sql(eval(f'f"{drop_stmt}"'))
  sql(eval(f'f"{create_stmt}"'))

# COMMAND ----------

col_rm = {
  'southridge_southridge': {
      'movies': ['_rid', '_self', '_etag', '_attachments', '_ts']
  }
}

# COMMAND ----------

col_adj = {
  "vanarsdelltd_onpremrentals_dbo": {
     'movies': {
       "ReleaseDate": "to_date({c},'MM-dd-yyyy') as {c}"
      }
    ,"transactions" : {
       "RentalDate": "to_date(string({c}),'yyyyMMdd') as {c}"
      ,"ReturnDate": "to_date(string({c}),'yyyyMMdd') as {c}"
     }
  }
 ,"fourthcoffee_rentals": {
     'movies': {
       "ReleaseDate": "to_date({c},'MM-dd-yyyy') as {c}"
      }
    ,"transactions" : {
       "RentalDate": "to_date(string({c}),'yyyyMMdd') as {c}"
      ,"ReturnDate": "to_date(string({c}),'yyyyMMdd') as {c}"
      ,"RewindFlag": "boolean({c})"
     }
  } 
 ,"southridge_southridge": {
     "movies": {
        "id": "concat_ws(':', {unified_src_id}, {c}) as MovieID"
       ,"title": "{c} as MovieTitle"
       ,"rating": "{c} as Rating"
       ,"runtime": "int({c}) as RunTimeMin"
       ,"genre": "{c} as Category"
     }
   }
 ,"southridge_cloudsales_dbo": {
     "orderdetails": {
        "MovieID": "concat_ws(':', {unified_src_id}, {c}) as {c}"
     }
   }  
 ,"southridge_cloudstreaming_dbo": {
     "transactions": {
        "MovieID": "concat_ws(':', {unified_src_id}, {c}) as {c}"
     }
   }    
}


# COMMAND ----------

import re

stmt = '''
create view {dst_db_name}.{table_name}
as
select {columns}
from   {src_db_name}.{table_name}
'''

src = {s.src_name: (s.src_id, s.unified_src_id) for s in spark.table('process_mng.src').collect()}

for d in sql("show databases like '*_x'").collect():
  
  src_db_name = d.databaseName
  src_name = d.databaseName[:-2]
  src_id, unified_src_id = src[src_name]
  dst_db_name = src_name + '_v'
  
  for t in sql(f"show tables in {src_db_name}").collect():
    
    table_name = t.tableName
    
    columns_x_table = spark.table(f'{src_db_name}.{table_name}').columns
    columns_rm = set(columns_x_table) - set(col_rm.get(src_name,{}).get(table_name,{}))
    columns_adj = [eval(f'f"{col_adj.get(src_name,{}).get(table_name,{}).get(c, c)}"') for c in columns_rm]      
    columns_id = [f"concat_ws(':',{src_id},{c}) as {c}" if re.fullmatch('\S+ID',c) else c for c in columns_adj]                                                     
    columns = ','.join([f'{src_id} as src_id'] + columns_id)
                              
    sql(eval(f"f'''{stmt}'''"))

# COMMAND ----------

for d in sql("show databases like '*_v'").collect():
  
  print('-'*20 + d.databaseName)
  
  for t in sql(f"show tables in {d.databaseName}").collect():
    
    print(t.tableName)

# COMMAND ----------

rows_per_table = 10

cell_sep = '\n\n' '-- COMMAND ----------' '\n'

for d in sql("show databases like '*_v'").collect():
  
  print(f'%md #{d.databaseName}{cell_sep}')
  
  for t in sql(f"show tables in {d.databaseName}").collect():
    
    print(f'-- DBTITLE 1,{t.tableName}')
    print(f'select * from {d.databaseName}.{t.tableName} limit {rows_per_table}{cell_sep}')