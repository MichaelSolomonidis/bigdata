from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Night Taxi Trips").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
user = "kkiousis"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{user}/Q4_NightTrips_{job_id}"

#αρχείο με τα ταξίδια από HDFS
data = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

#προσωρινό πίνακα για SQL
data.createOrReplaceTempView("trips")

#διαδρομές που ξεκίνησαν από 23:00 έως 06:59
query = """
SELECT 
  VendorID, 
  COUNT(*) AS NightTrips
FROM trips
WHERE HOUR(tpep_pickup_datetime) >= 23 OR HOUR(tpep_pickup_datetime) < 7
GROUP BY VendorID
"""


night_stats = spark.sql(query)
night_stats.show()
night_stats.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
