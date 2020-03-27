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

types_doc_to_synapse_stg = {
  '16-bit integer': 'smallint'
 ,'32-bit integer': 'int'
 ,'8-bit integer': 'tinyint'
 ,'boolean': 'bit'
 ,'currency': 'money'
 ,'date': 'date'
 ,'decimal': 'decimal(14,4)'
 ,'guid': 'varchar(50)'
 ,'string': 'string'
 ,'time': 'char(8)'
}  

# COMMAND ----------

import re

stmt = 'create table stg.{table_name} ({schema}) with (distribution = round_robin, heap);'

for table_name, columns in tables:
  
  columns_schema = []
  
  for c in columns:
    
    column_name = c.column_name
    data_type = types_doc_to_synapse_stg[c.data_type.lower()]
    null = {'no': 'not null', 'yes':'null'}[c.nulls.lower()]
    
    if data_type == 'string':
      
      instruction = re.match('.*?(single|exactly|max|$)', c.rules, flags = re.IGNORECASE).group(1).lower()
      str_len = re.match('.*\(.*?(\d+)|', c.rules).group(1)
      data_type = {'single': 'char(1)', 'exactly': f'char({str_len})', 'max': f'varchar({str_len})'}.get(instruction, 'varchar(50)')
       
    column_schema = f'{column_name} {data_type} {null}'      
    columns_schema.append(column_schema) 
  
  schema = ','.join(columns_schema)
  print(eval(f'f"{stmt}"'))

# COMMAND ----------

types_doc_to_synapse_dwh = {
  '16-bit integer': 'smallint'
 ,'32-bit integer': 'int'
 ,'8-bit integer': 'tinyint'
 ,'boolean': 'bit'
 ,'currency': 'money'
 ,'date': 'date'
 ,'decimal': 'decimal(14,4)'
 ,'guid': 'varchar(50)'
 ,'string': 'string'
 ,'time': 'time(0)'
}  

# COMMAND ----------

import re

stmt = 'create table dwh.{table_name} ({schema}) with (distribution = {distribution}, clustered columnstore index);'

for table_name, columns in tables:
  
  first_column = columns[0].column_name
  distribution = 'replicate' if table_name.startswith('Dim') else f'hash ({first_column})'
  
  columns_schema = []
  
  for c in columns:
    
    column_name = c.column_name
    data_type = types_doc_to_synapse_stg[c.data_type.lower()]
    null = {'no': 'not null', 'yes':'null'}[c.nulls.lower()]
    
    if data_type == 'string':
      
      instruction = re.match('.*?(single|exactly|max|$)', c.rules, flags = re.IGNORECASE).group(1).lower()
      str_len = re.match('.*\(.*?(\d+)|', c.rules).group(1)
      data_type = {'single': 'char(1)', 'exactly': f'char({str_len})', 'max': f'varchar({str_len})'}.get(instruction, 'varchar(50)')
       
    column_schema = f'{column_name} {data_type} {null}'      
    columns_schema.append(column_schema) 
  
  schema = ','.join(columns_schema)
  print(eval(f'f"{stmt}"'))