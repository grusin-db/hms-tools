import functools
from typing import List, Dict, Callable, Any, Union
from datetime import datetime
from joblib import Parallel, delayed

def _validate_list_of_type(obj, obj_name, item_type, default_value=None):
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
  return [
    r.databaseName
    for r in spark.sql(f'show databases').collect()
  ]

def get_tables(databases:Union[str,List[str]]=None, max_workers=16) -> Union[str, Exception]:
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