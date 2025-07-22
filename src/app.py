from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark =SparkSession.builder.appName("uber-data-analysis").master("local[*]").getOrCreate()
df = spark.read.csv("data/dataset.csv", inferSchema=True, header=True)

spark.conf.set("spark.sql.session.timeZone", "UTC")        # optional
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")  # helps parse 2-digit years
spark.conf.set("spark.sql.session.locale", "en")           # <--- this is the key!

df = df.withColumn("Date", to_date(unix_timestamp("date", "dd-MMM-yy").cast("timestamp")))
w=Window.rowsBetween(Window.unboundedPreceding, 0)
df = df.withColumn("Date", last("Date", ignorenulls=True).over(w))
##########################################################################################################################
print("Q1====>Which date had the most completed trips during the two-week period ?")
# clean rows with NULL dates
clean_df = df.dropna(subset=["Date"])
completed_trips_sum = clean_df.groupBy("Date").agg(sum(col("Completed Trips ")).alias("total_trips"))
# Way 1
in_desc = completed_trips_sum.sort(col("total_trips").desc()).limit(1).select("Date").first()["Date"]
print("SOL1====>", in_desc)
# Way 2
sol_1=completed_trips_sum.sort(col("total_trips").desc()).select("Date").first()["Date"]
print("SOL====>", sol_1)
###########################################################################################################################
print("Q2====>What was the highest number of completed trips within a 24 hour period")

desc_sort_df = df.groupBy("Date").agg(sum("completed trips ").alias("total_complete")).sort(col("total_complete").desc())
sol_2 = desc_sort_df.first()["total_complete"]
print("SOL2====>", sol_2)

###############################################################################################################
print("Q3====>Which hour of the day had the most requests during the two week period?")
sort_desc_hourly_request = df.groupBy("Time (Local)").agg(sum("Requests ").alias("total")).sort(col("total").desc())
sol_3 = sort_desc_hourly_request.first()["Time (Local)"]
print("SOL3====>", sol_3)
###############################################################################################################
print("Q4====>What percentages of all zeroes during the two week period occurred on weekend "
"(Friday at 5 pm to Sunday at 3 am)? Tip: The local time value is the start of the hour "
"(e.g. 15 is the hour from 3:00pm - 4:00pm)")
numerator = df.filter(((col("Time (Local)")>=17) | (col("Time (Local)")< 3))).filter(((weekday(col("Date"))==4) |\
                                                                    (weekday(col("Date"))==5) | (weekday(col("Date"))==6))).agg(sum("Zeroes ").alias("req_zero")).first()["req_zero"]

denominator = df.agg(sum("Zeroes ").alias("total_zero")).first()["total_zero"]

print("SOL4====>", (numerator/denominator) * 100)
####################################################################################################################
# print("====>Q5. What is the weighted average ratio of completed trips per driver during the two week period? " \
# "Tip: Weighted average means your answer should account for the total trip volume in each hour to determine " \
# "the most accurate number in whole period.")
# wighted_ratio = df.withColumn("completed_per_trip", df["Completed Trips "]/df["Unique Drivers"])\
#     .groupBy("Date", "Time (Local)")

# weighted_avg = df.withColumn("completed_per_driver", df["Completed Trips "] / df["Unique Drivers"]) \
#                  .groupBy("Date", "Time (Local)") \
#                  .agg(avg("completed_per_driver").alias("avg_completed_per_driver"), sum("Completed Trips ").alias("total_completed_trips")) \
#                  .withColumn("weighted_ratio", col("avg_completed_per_driver") * col("total_completed_trips")) \
#                  .agg(sum("weighted_ratio") / sum("total_completed_trips")).collect()[0][0]
##################################################################################################################
print("Q6=====>In drafting a driver schedule in terms of 8 hours shifts, when are the busiest 8 " \
"consecutive hours over the two week period in terms of unique requests? A new shift starts in every 8 hours. " \
"Assume that a driver will work same shift each day.")
hourly_unique_request=df.groupBy("Time (Local)").agg(count_distinct("Requests ").alias("unique_requests"))
win=Window.orderBy(col("Time (Local)").asc()).rowsBetween(0,7)
busiest_8_consecutive_hours = hourly_unique_request.select("*", sum("unique_requests").over(win)\
                                                           .alias("consecutive_sum"))\
                                                            .sort(col("consecutive_sum").desc()).first()["Time (Local)"]
print("SOL6====> from ", busiest_8_consecutive_hours, "to", (busiest_8_consecutive_hours+8) % 24)
###################################################################################################################
print("Q7====>In which 72 hour period is the ratio of Zeroes to Eyeballs the highest?")
sol_7 = df.groupBy(
    (
        (col("date").cast("timestamp").cast("long")/72*3600)
    ).alias("72_hour"))\
    .agg(sum("Zeroes ").alias("sum_zero"), sum("Eyeballs ").alias("sum_eyeballs")).withColumn("ratio", col("sum_zero")/col("sum_eyeballs")).sort(col("ratio").desc()).first()["ratio"]
print("SOL7======>", sol_7)
#######################################################################################################################
print("Q8====>If you could add 5 drivers to any single hour of every day during the two week period, " \
"which hour should you add them to? Hint: Consider both rider eyeballs and driver supply when choosing")
non_zero_drivers = df.filter(col("Unique Drivers")!=0)
sol_8 = non_zero_drivers.groupBy("Time (Local)").agg(sum(col("Requests ")/col("Unique Drivers")).alias("req_per_driver")).sort(col("req_per_driver").desc()).first()["Time (Local)"]
print("SOL8====>", sol_8)
########################################################################################################################
print("Q9====>Looking at the data from all two weeks, which time might make the most sense to" \
" consider a true 'end day' instead of midnight? (i.e when are supply and demand at both their natural minimums) " \
"Tip: Visualize the data to confirm your answer if needed.")
vis_df = df.groupBy("Time (Local)").agg(avg("Completed Trips ").alias("avg_compl_trips"),\
                                         avg("Unique Drivers").alias("avg_uniq_drv"))\
                                            .sort(["avg_compl_trips", "avg_uniq_drv"], ascending = [1, 1]).first()["Time (Local)"]

print("SOL9=====>", vis_df)