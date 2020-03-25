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

# DBTITLE 1,Drop views databases
stmt= "drop database if exists {db_name} cascade"

for p in paths:
  db_name = p.replace('/','_') + '_v'
  sql(eval(f'f"{stmt}"'))

# COMMAND ----------

# DBTITLE 1,Create views databases
stmt= "create database if not exists {db_name} location '{mount_point}/dummy/{db_name}'"

for p in paths:
  db_name = p.replace('/','_') + '_v'
  sql(eval(f'f"{stmt}"'))

# COMMAND ----------

src_ids = {d.databaseName[:-2]: i for i, d in enumerate(sql("show databases like '*_x'").collect(), start = 1)}
print(src_ids)

# COMMAND ----------

col_rm = {
  'southridge_southridge': {
      'movies': ['actors', '_rid', '_self', '_etag', '_attachments', '_ts']
  }
 ,'vanarsdelltd_onpremrentals_dbo': {
      'customers': ['AddressLine1','AddressLine2','City','State','ZipCode']
  }
 ,'fourthcoffee_rentals': {
      'customers': ['AddressLine1','AddressLine2','City','State','ZipCode']
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
        "id": "{c} as MovieID"
       ,"title": "{c} as MovieTitle"
       ,"rating": "{c} as Rating"
       ,"runtime": "int({c}) as RunTimeMin"
       ,"genre": "{c} as Category"
     }
   }
}


# COMMAND ----------

stmt = '''
create view {dst_db_name}.{table_name}
as
select {columns}
from   {src_db_name}.{table_name}
'''


for d in sql("show databases like '*_x'").collect():
  
  src_db_name = d.databaseName
  base_db_name = d.databaseName[:-2]
  dst_db_name = base_db_name + '_v'
   
  for t in sql(f"show tables in {src_db_name}").collect():
    
    table_name = t.tableName
    
    columns_x_tab = spark.table(f'{src_db_name}.{table_name}').columns
    columns_rm = set(columns_x_tab) - set(col_rm.get(base_db_name,{}).get(table_name,{}))
    columns_adj = [eval(f'f"{col_adj.get(base_db_name,{}).get(table_name,{}).get(c, c)}"') for c in columns_rm]
    columns = ','.join([f'{src_ids[base_db_name]} as src_id'] + columns_adj)
                              
    sql(eval(f"f'''{stmt}'''"))

# COMMAND ----------

# DBTITLE 1,southridge_southridge_v.actors
src_id = src_ids['southridge_southridge']

stmt = f"""
create view if not exists southridge_southridge_v.actors
as
select   {src_id} as src_id
        ,a.ActorName

from     southridge_southridge_x.movies
         lateral view inline(from_json(actors, 'array<struct<name:string>>')) a as ActorName
"""

sql(stmt)

# COMMAND ----------

# DBTITLE 1,vanarsdelltd_onpremrentals_dbo_v.addresses
src_id = src_ids['vanarsdelltd_onpremrentals_dbo']

stmt = f"""
create view if not exists vanarsdelltd_onpremrentals_dbo_v.addresses
as
select   {src_id} as src_id
        ,CustomerID, CreatedDate, UpdatedDate
        ,AddressLine1 ,AddressLine2 ,City ,State ,ZipCode
        
from     vanarsdelltd_onpremrentals_dbo_x.customers
"""

sql(stmt)

# COMMAND ----------

# DBTITLE 1,fourthcoffee_rentals_v.addresses
src_id = src_ids['fourthcoffee_rentals']

stmt = f"""
create view if not exists fourthcoffee_rentals_v.addresses
as
select   {src_id} as src_id
        ,CustomerID, CreatedDate, UpdatedDate
        ,AddressLine1 ,AddressLine2 ,City ,State ,ZipCode
        
from     fourthcoffee_rentals_x.customers
"""

sql(stmt)

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