from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Q5 Zone Analysis").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
user = "kkiousis"
job_id = sc.applicationId
output_path = f"hdfs://hdfs-namenode:9000/user/{user}/Q5_Humanized_{job_id}"

#αρχείο διαδρομών ταξί
trips = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv") \
    .select("PULocationID", "DOLocationID")

#αρχείο περιοχών με borough και zone
zones = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv") \
    .select("LocationID", "Borough", "Zone")

#join για το pickup location με πληροφορίες ζώνης
trips_with_pickup = trips.join(
    zones.withColumnRenamed("LocationID", "PU_ID")
         .withColumnRenamed("Borough", "PickupBorough")
         .withColumnRenamed("Zone", "PickupZone"),
    trips["PULocationID"] == col("PU_ID")
)

#join για το dropoff location με πληροφορίες ζώνης
trips_full = trips_with_pickup.join(
    zones.withColumnRenamed("LocationID", "DO_ID")
         .withColumnRenamed("Borough", "DropoffBorough")
         .withColumnRenamed("Zone", "DropoffZone"),
    trips_with_pickup["DOLocationID"] == col("DO_ID")
)

#διαδρομές με διαφορετικές ζώνες αλλά ίδιο borough
zone_diff = trips_full.filter(
    (col("PickupZone") != col("DropoffZone")) &
    (~col("PickupBorough").isin("Unknown", "N/A"))
)

#ζεύγος Pickup/Dropoff Zone και μέτρηση διαδρομών
result = zone_diff.groupBy("PickupZone", "DropoffZone") \
    .count() \
    .withColumnRenamed("count", "TotalTrips") \
    .orderBy(col("TotalTrips").desc())


result.show(4) #4 πρώτα αποτελέσματα
result.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
