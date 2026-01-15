import logging 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, ArrayType, StructType, StructField, DateType




spark = SparkSession.builder.appName("TMDB_Cleaning").getOrCreate()

data_path = "/tmdbmovies/app/data/raw/tmdb_movies_raw.csv"


df = spark.read.option("header",True).csv(data_path)

df.show()