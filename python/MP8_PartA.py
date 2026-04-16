from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

# import urllib3 # this violates the don't import libraries rule
# import urllib # this doesn't but does it work w/ python3?
# import urllib.request

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

# url = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-1gram-20120701-a.gz"

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)


# Spark SQL - DataFrame API

rdd = sc.textFile("gbooks")

# ["word", "year", "frequency", "books"]
schema = StructType([
   StructField("word", StringType(), True),
   StructField("year", IntegerType(), True),
   StructField("frequency", IntegerType(), True),
   StructField("books", IntegerType(), True)])

df = spark.createDataFrame(rdd, schema)

df.printSchema()


