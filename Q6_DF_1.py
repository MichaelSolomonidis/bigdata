from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg


spark = SparkSession.builder.appName("Hourly Pickup Location").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
user = "kkiousis"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{user}/Q1_Humanized_{job_id}"

#αρχείο με ταξίδια (έτος 2015)
data = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

#απαραίτα πεδία
pickups = data.select("tpep_pickup_datetime", "pickup_longitude", "pickup_latitude")

#κρατάω τις έγκυρες γεωγραφικές τιμές
valid_pickups = pickups.filter(
    (col("pickup_longitude") != 0) & 
    (col("pickup_latitude") != 0)
)

#ώρα από datetime
with_hour = valid_pickups.withColumn("hour", hour(col("tpep_pickup_datetime")))

#υπολογισμός μέσων τιμών συντεταγμένων ανά ώρα
hourly_avg = with_hour.groupBy("hour").agg(
    avg("pickup_longitude").alias("Longitude"),
    avg("pickup_latitude").alias("Latitude")
).orderBy("hour")

#μετονομασία στήλης ώρας
result = hourly_avg.withColumnRenamed("hour", "HourOfDay")
result.show()
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
