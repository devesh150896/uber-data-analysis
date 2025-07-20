from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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
print("Q2. What was the highest number of completed trips within a 24 hour period")
w=Window.rowsBetween(Window.unboundedPreceding, 0)
filled_df = df.withColumn("Date_Filled", last("Date", ignorenulls=True).over(w))
desc_sort_df = filled_df.groupBy("Date_Filled").agg(sum("completed trips ").alias("total_complete")).sort(col("total_complete").desc())
sol_2 = desc_sort_df.first()["total_complete"]
print(sol_2)

###############################################################################################################
# print("Which hour of the day had the most requests during the two week period?")
# # df.groupBy("Time (Local)").agg(sum("Requests ").alias("total")).sort(col("total").desc()).show()
# hourly_requests = df.groupBy("Time (Local)").agg(sum("Requests").alias("total_requests")).orderBy("total_requests", ascending=False)

# most_requested_hour = hourly_requests.select("hour").first()[0]
# print("The hour with the most requests is:", most_requested_hour)