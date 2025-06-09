from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Taxi Borough Analysis").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
username = "kkiousis"
save_path = f"hdfs://hdfs-namenode:9000/user/{username}/Q3_Modified_SQL"

#δεδομένα διαδρομών ταξί
rides = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv") \
    .select("PULocationID", "DOLocationID")

#πίνακες με τα zones
zone_info = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv") \
    .select("LocationID", "Borough")

#πίνακες για SQL queries
rides.createOrReplaceTempView("ride_data")
zone_info.createOrReplaceTempView("zone_data")

#υπολογισμός διαδρομών που έγιναν στο ίδιο borough
sql_query = """
SELECT 
    z_start.Borough AS BoroughName,
    COUNT(1) AS NumTrips
FROM ride_data r
JOIN zone_data z_start ON r.PULocationID = z_start.LocationID
JOIN zone_data z_end ON r.DOLocationID = z_end.LocationID
WHERE z_start.Borough = z_end.Borough
  AND z_start.Borough NOT IN ('Unknown', 'N/A')
GROUP BY z_start.Borough
ORDER BY NumTrips DESC
"""

borough_stats = spark.sql(sql_query)
borough_stats.show()
borough_stats.coalesce(1).write.mode("overwrite").option("header", True).csv(save_path)
