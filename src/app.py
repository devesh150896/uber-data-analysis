from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark =SparkSession.builder.appName("uber-data-analysis").master("local[*]").getOrCreate()
df = spark.read.csv("data/dataset.csv", inferSchema=True, header=True)
df.show()

##########################################################################################################################
print("Q1. Which date had the most completed trips during the two-week period ?")
# clean rows with NULL dates
clean_df = df.dropna(subset=["Date"])
completed_trips_sum = clean_df.groupBy("Date").agg(sum(col("Completed Trips ")).alias("total_trips"))
# Way 1
in_desc = completed_trips_sum.sort(col("total_trips").desc()).limit(1).select("Date").first()["Date"]
print(in_desc)
# Way 2
sol_1=completed_trips_sum.sort(col("total_trips").desc()).select("Date").first()["Date"]
print(sol_1)
###########################################################################################################################