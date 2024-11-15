from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, current_date, date_sub,to_date,date_format,max
import os
from pyspark.sql.window import Window

if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("Earthquake Analysis") \
        .master("local[*]")\
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.0") \
        .getOrCreate()

    df = (spark.read
              .format("bigquery")
              .option("table", "seismic-operand-441006-c9.earthquake_df.earthquake_data_pycharm")
              .load())

    df = df.withColumn("date", to_date(df["updated"]))
    # df.show()

    # 1.Count the number of earthquakes by region

    df1 = df.groupBy("area").agg(count("*").alias("earthquake_count_by_region"))
    # df1.show()



    # 2. Find the average magnitude  by the region

    df2 = df.groupBy('area').agg(avg('mag').alias('avg_mag'))

    # df2.show()
    #
    # 3. Find how many earthquakes happen on the same day.


    df3 = df.groupBy('date').agg(count('*').alias('num_of_eq'))
    # df3.show()

    #
    # 4. Find how many earthquakes happen on same day and in same region

    df4 = df.groupBy('date','area').agg(count('*').alias('num_of_eq'))
    # df4.show(1000)

    #
    # 5. Find average earthquakes happen on the same day.
    df5 = df.groupBy("date").agg(count("*").alias("earthquake_count_per_day")).agg(avg("earthquake_count_per_day").alias("average_earthquakes_same_day"))
    # df5.show()
    #

    # 6.Find average earthquakes happen on same day and in same region
    window_spec = Window.partitionBy("date", "area")

    df6 = (
        df.groupBy("date", "area")
        .agg(count("*").alias("earthquake_count"))
        .withColumn("average_earthquakes_same_day_same_region", avg("earthquake_count").over(window_spec))
    )

    # df6.show(1000)



    # 7. Find the region name, which had the highest magnitude earthquake last week.

    last_week = df.filter(col("date") >= date_sub(current_date(), 7))
    df7 = last_week \
        .groupBy("area") \
        .agg(max("mag").alias("highest_magnitude_last_week")) \
        .orderBy(col("highest_magnitude_last_week").desc()) \
        .limit(1)

    df7.show()



    # 8. Find the region name, which is having magnitudes higher than 5.

    df8 = df.select('area','mag').filter(col('mag')>5)
    # df8.show()

    # 9. Find out the regions which are having the highest frequency and intensity of earthquakes.

    df9 = df.groupBy("area").agg(count("*").alias("highest_frequency"),
                                                 max("mag").alias("highest_intensity")) \
        .orderBy(col("highest_frequency").desc(), col("highest_intensity").desc())\
        .limit(5)

    # df9.show()
