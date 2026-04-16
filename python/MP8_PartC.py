from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

from pyspark.sql.functions import col

sc = SparkContext()
spark = SparkSession.builder.getOrCreate()

####
# 1. Setup : Write a function to load it in an RDD & DataFrame
####

# RDD API
# Columns:
# 0: word (string), 1: year (int), 2: frequency (int), 3: books (int)

rdd = sc.textFile("gbooks")
cols = rdd.map(lambda line: line.split("\t"))

# Spark SQL - DataFrame API

df = cols.toDF(["word", "year", "frequency", "books"])

df = df.withColumn("year", col("year").cast(IntegerType()))
df = df.withColumn("frequency", col("frequency").cast(IntegerType()))
df = df.withColumn("books", col("books").cast(IntegerType()))

####
# 3. Filtering : Count the number of appearances of word 'ATTRIBUTE'
####

# Spark SQL

# +--------+
# |count(1)|
# +--------+
# |      11|
# +--------+


result_df = spark.sql("SELECT COUNT(*) FROM {df_param} WHERE word = 'ATTRIBUTE'", df_param=df)

result_df.show()

