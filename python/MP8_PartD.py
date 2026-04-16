from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

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
#  4. MapReduce : List the top three words that have appeared in the
#  greatest number of years. 
#  Sorting order of the final answer should should be descending by word count,
#  then descending by word.

# Spark SQL

# +-------------+--------+
# |         word|count(1)|
# +-------------+--------+
# |    ATTRIBUTE|      11|
# |approximation|       4|
# |    agast_ADV|       4|
# +-------------+--------+
# only showing top 3 rows


result_df = spark.sql("SELECT word, COUNT(*) FROM {df_param} GROUP BY word, year, frequency, books ORDER BY COUNT(*) LIMIT 3", df_param=df)

result_df.show()