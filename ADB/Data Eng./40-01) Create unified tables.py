# Databricks notebook source
import requests 
from bs4 import BeautifulSoup

class column_metadata:
    def __init__(self, column_name, data_type, nulls, rules):
        self.column_name = column_name
        self.data_type   = data_type
        self.nulls       = nulls
        self.rules       = rules

url = 'https://gist.github.com/riserrad/fabf9103ac1d8a383f16a1feee54a73a'
result = requests.get(url)
soup = BeautifulSoup(result.text, 'html.parser')

tables = []

for table in soup.select("table"):
    
    table_name = table.findPreviousSiblings('h2', limit = 1)[0].text
    tbody = table.select("tbody")[0]
    columns = [column_metadata(*(td.text for td in tr.select("td"))) for tr in tbody.select("tr")]
    tables.append((table_name, columns))
    


# COMMAND ----------

tables

# COMMAND ----------

set(c.data_type.lower() for table_name, columns in tables for c in columns)

# COMMAND ----------

types_doc_to_spark = {
  '16-bit integer': 'smallint'
 ,'32-bit integer': 'int'
 ,'8-bit integer': 'tinyint'
 ,'boolean': 'boolean'
 ,'currency': 'decimal(19,4)'
 ,'date': 'date'
 ,'decimal': 'decimal(14,4)'
 ,'guid': 'string'
 ,'string': 'string'
 ,'time': 'string'
}  

# COMMAND ----------

# MAGIC %sql
# MAGIC drop database if exists unified cascade;
# MAGIC create database if not exists unified location '/mnt/dumarkovopenhackmdw/databases/unified'

# COMMAND ----------

stmt = 'create table unified.{table_name} ({schema}) using delta'

for table_name, columns in tables:
  schema_parts = [(c.column_name, types_doc_to_spark[c.data_type.lower()], c.rules.replace("'",r"\'")) for c in columns]
  schema = ','.join([f"{name} {type} comment '{comment}'" for name, type, comment in schema_parts])
  sql(eval(f'f"{stmt}"'))

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in unified

# COMMAND ----------

# DBTITLE 1,Generate unified desc
cell_sep = '\n\n' '-- COMMAND ----------' '\n'

for t in sql(f"show tables in unified").collect():

  print(f'-- DBTITLE 1,{t.tableName}')
  print(f'desc unified.{t.tableName}{cell_sep}')