# Databricks notebook source
# MAGIC %md # Run this notebook cluster with connection to source (external) HMS

# COMMAND ----------

# DBTITLE 1,helper functions
import functools
from typing import List, Dict, Callable, Any, Union
from datetime import datetime
from joblib import Parallel, delayed

def _validate_list_of_type(obj, obj_name, item_type, default_value=None):
  """validates of obj is a list of given item_type, if not error having user friendly error name quoting obj_name will be raised"""
  if obj is None:
    obj = default_value
  
  if obj is None:
    raise ValueError(f"{obj_name} is not defined (debug; {default_value=})")

  if isinstance(obj, tuple):
    obj = list(obj)

  if item_type != Any:
    if isinstance(obj, item_type):
      obj = [ obj ]
  else:
    if not isinstance(obj, list):
      obj = [ obj ]

  if not isinstance(obj, list):
    raise ValueError(f"{obj_name} must be a list of {item_type}")

  if item_type != Any:
    for r in obj:
      if not isinstance(r, item_type):
        raise ValueError(f"{obj_name} must be a list of {item_type}")
    
  return obj

def get_databases() -> List[str]:
  """gets list of all databases"""
  return [
    r.databaseName
    for r in spark.sql(f'show databases').collect()
  ]

def get_tables(databases:Union[str,List[str]]=None, max_workers=16) -> Union[str, Exception]:
  """gets list of tables in databases, utilizes multithreading using default 16 max_workers
     returns list, where each element is:
     - string in format database_name.table_name if table was found
     - or exception if database contents could not be read
  """
  databases = _validate_list_of_type(databases, "databases", str, [])
    
  if not databases:
    return []

  if len(databases) == 1:
    try:
      return [
        f"{r.database}.{r.tableName}"
        for r in spark.sql(f"show tables in {databases[0]}").collect()
        if r.isTemporary == False
      ]
    except Exception as e:
      return e

  # more than one? make it parallel
  data = Parallel(n_jobs=max_workers, backend='threading')(
    delayed(get_tables)(name) 
    for name in databases
  )

  return [
    x2
    for x in data
    for x2 in x
  ]

def get_create_table_scripts(tables:Union[str, List[str]]=None, max_workers=16) -> Dict[str, Union[str, Exception]]:
  """gets list of create table scripts, utilizes multithreading using default 16 max_workers
     returns list, where each element is:
     - string in format of hive create table statement
     - or exception error occured
  """
  tables = _validate_list_of_type(tables, "tables", str, [])
  
  if not tables:
    return []

  if len(tables) == 1:
    try:
      return spark.sql(f"SHOW CREATE TABLE {tables[0]}").first()[0]
    except Exception as e:
      return e
    
  # more than one? make it parallel
  results = Parallel(n_jobs=max_workers, backend='threading')(
    delayed(get_create_table_scripts)(name) 
    for name in tables
  )

  return dict(zip(tables, results))

# COMMAND ----------

# DBTITLE 1,get databases to convert
# remove [:20] to get all of them, or manually provide list of db_names to export
db_names = get_databases()[:20]
db_names

# COMMAND ----------

# DBTITLE 1,get tables names in these databases
table_names = get_tables(db_names, max_workers=16)
table_names

# COMMAND ----------

# DBTITLE 1,get table create scripts, or exception if table is invalid
create_tables_scripts = get_create_table_scripts(table_names, max_workers=16)
create_tables_scripts

# COMMAND ----------

# DBTITLE 1,split between good and bad tables
good_scripts = {}
bad_scripts = {}

for name, result in create_tables_scripts.items():
  if isinstance(result, Exception):
    bad_scripts[name] = result
  else:
    good_scripts[name] = result

print("good:", len(good_scripts))
print("bad:", len(bad_scripts))

# COMMAND ----------

# DBTITLE 1,nicely print errors
for name, excption in bad_scripts.items():
  banner = "#"*(len(name)+12)
  print(banner)
  print("#####", name, "#####")
  print(banner)
  print(str(excption) + "\n\n")

# COMMAND ----------

# DBTITLE 1,fail if errors were found
if len(bad_scripts) > 0:
  raise ValueError("some tables failed, fix them and try again...")fr 

# COMMAND ----------

# DBTITLE 1,Optionally add CREATE OR REPLACE TABLE instead of CREATE TABLE
good_scripts = {
  name: script.replace("CREATE TABLE", "CREATE OR REPLACE TABLE", 1)
  for name, script in good_scripts.items()
}

# COMMAND ----------

import json

good_json = json.dumps(good_scripts, indent=2)

dbutils.fs.put('dbfs:/hms-export/tables_and_views.json', good_json)

# COMMAND ----------

# DBTITLE 1,verify written file
# MAGIC %fs head dbfs:/hms-export/tables_and_views.json

# COMMAND ----------


