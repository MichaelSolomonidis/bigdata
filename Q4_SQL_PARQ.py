from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Night Trips SQL PARQ").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
user = "kkiousis"
job = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{user}/Q4_NightTrips_Parquet_{job}"

#CSV -> Parquet -> ανάγνωση
csv_path = "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv"
parquet_path = f"hdfs://hdfs-namenode:9000/user/{user}/rides_2024.parquet"

#αρχείο CSV αποθήκευση ως Parquet
rides = spark.read.option("header", True).option("inferSchema", True).csv(csv_path)
rides.write.mode("overwrite").parquet(parquet_path)

#φόρτωση από Parquet
rides_parquet = spark.read.parquet(parquet_path)

#προσωρινος πίνακας για SQL
rides_parquet.createOrReplaceTempView("trips")

#υπολογισμός διαδρομών που ξεκίνησαν μεταξύ 23:00 και 06:59
query = """
SELECT 
  VendorID, 
  COUNT(*) AS NightTrips
FROM trips
WHERE HOUR(tpep_pickup_datetime) >= 23 OR HOUR(tpep_pickup_datetime) < 7
GROUP BY VendorID
"""


results = spark.sql(query)
results.show()
results.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
