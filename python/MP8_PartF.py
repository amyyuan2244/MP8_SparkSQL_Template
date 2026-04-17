from pyspark.sql.functions import col, lag, lead
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

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


df = cols.toDF(["word", "year", "frequency", "books"])

df = df.withColumn("year", col("year").cast(IntegerType()))
df = df.withColumn("frequency", col("frequency").cast(IntegerType()))
df = df.withColumn("books", col("books").cast(IntegerType()))

###
# 2. Frequency Increase : analyze the frequency increase of words starting from the year 1500 to the year 2000
###
# Spark SQL - DataFrame API

# filter to just years up to and including 2000
df_2000 = spark.sql("SELECT * FROM {df_param} WHERE year <= 2000", df_param=df)

window = Window.partitionBy("word").orderBy("year")

df_freq = df_2000.withColumn("frequency_increase", lead("frequency", default=0).over(window))
# df = df.withColumn("frequency_increase", lag("frequency", offset=-1, default=0).over(window))

df_word_increase = spark.sql("SELECT word, SUM(frequency_increase) AS total_increase FROM {df_param} GROUP BY word ORDER BY total_increase DESC", df_param=df_freq)


df_word_increase.show(20)