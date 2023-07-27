__version__ = "0.1.0"

from pyspark.sql import SparkSession
spark:SparkSession = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
table = spark.table
sql = spark.sql

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)



