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
# 5. Joining : The following program construct a new dataframe out of 'df' with a much smaller size.
####

df2 = df.select("word", "year").distinct().orderBy("year", "word").limit(100)
df2.createOrReplaceTempView('gbooks2')

# Now we are going to perform a JOIN operation on 'df2'. 
# Do a self-join on 'df2' in lines with the same #'count1' values 
# and see how many lines this JOIN could produce.
#  Answer this question via Spark SQL API

# Spark SQL API

result_df = spark.sql("SELECT COUNT(*) FROM {df2_param} T1, {df2_param} T2 WHERE T1.year = T2.year", df2_param=df2)

print(result_df.head()[0])

# output: 310

