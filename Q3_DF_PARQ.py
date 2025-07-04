from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("Taxi Trips Same Borough").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
username = "kkiousis"
output_path = f"hdfs://hdfs-namenode:9000/user/{username}/Q3_Result"

#αρχείο με τα ταξί
trips = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv") \
    .select("PULocationID", "DOLocationID")

#περιοχές και τα boroughs
zones = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv") \
    .select("LocationID", "Borough")

#join για να βρω το borough για το pickup
trips_with_pickup = trips.join(
    zones.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Borough", "PickupBorough"),
    on="PULocationID"
)

#join για να βρω το borough για το dropoff
trips_with_both = trips_with_pickup.join(
    zones.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Borough", "DropoffBorough"),
    on="DOLocationID"
)

#ταξίδια που ξεκινάνε και τελειώνουν στο ίδιο borough
#αποφεύγουμε και τα άγνωστα/κενά
same_borough_trips = trips_with_both.filter(
    (col("PickupBorough") == col("DropoffBorough")) &
    (~col("PickupBorough").isin("Unknown", "N/A"))
)

#μετράω πόσα τέτοια ταξίδια υπάρχουν
result = same_borough_trips.groupBy("PickupBorough") \
    .agg(count("*").alias("TotalTrips")) \
    .orderBy(col("TotalTrips").desc()) \
    .withColumnRenamed("PickupBorough", "Borough")

result.show()

result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
