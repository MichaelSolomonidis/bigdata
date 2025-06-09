from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Taxi Same Borough Stats").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
user = "kkiousis"
job = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{user}/Q3_Humanized_SQL_PARQ_{job}"

#ταξίδια από CSV τα γράφουμε ως Parquet
rides_csv = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv"
zones_csv = "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv"
rides_parquet = f"hdfs://hdfs-namenode:9000/user/{user}/rides_2024.parquet"
zones_parquet = f"hdfs://hdfs-namenode:9000/user/{user}/zones.parquet"

#αποθήκευση σε Parquet
rides = spark.read.option("header", True).option("inferSchema", True).csv(rides_csv)
zones = spark.read.option("header", True).option("inferSchema", True).csv(zones_csv)
rides.write.mode("overwrite").parquet(rides_parquet)
zones.write.mode("overwrite").parquet(zones_parquet)

#κρατάμε μόνο τις απαραίτητες στήλες
rides_df = spark.read.parquet(rides_parquet).select("PULocationID", "DOLocationID")
zones_df = spark.read.parquet(zones_parquet).select("LocationID", "Borough")

#πίνακες για SQL
rides_df.createOrReplaceTempView("ride_table")
zones_df.createOrReplaceTempView("zone_table")

#βρίσκουμε πόσα ταξίδια έγιναν μέσα στο ίδιο Borough
query = """
SELECT 
  origin.Borough AS Borough,
  COUNT(*) AS TotalTrips
FROM ride_table r
JOIN zone_table origin ON r.PULocationID = origin.LocationID
JOIN zone_table dest ON r.DOLocationID = dest.LocationID
WHERE origin.Borough = dest.Borough
  AND origin.Borough NOT IN ('Unknown', 'N/A')
GROUP BY origin.Borough
ORDER BY TotalTrips DESC
"""


results = spark.sql(query)
results.show()
results.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
