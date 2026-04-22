"""
Spark Job: Bronze → Silver
Clean, deduplicate, cast types, partition user_logs by date.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("KKBox Bronze to Silver") \
    .getOrCreate()

BUCKET = "gs://kkbox-churn-prediction-493716-data"
BRONZE = BUCKET + "/bronze/raw"
SILVER = BUCKET + "/silver"

# members
print("Processing members...")
members = spark.read.csv(BRONZE + "/members_v3.csv", header=True, inferSchema=True)
members = members \
    .dropDuplicates(["msno"]) \
    .withColumn("bd", F.when((F.col("bd") < 1) | (F.col("bd") > 100), None).otherwise(F.col("bd"))) \
    .withColumn("registration_init_time", F.to_date(F.col("registration_init_time").cast("string"), "yyyyMMdd"))
members.write.mode("overwrite").parquet(SILVER + "/members")
print("members done")

# transactions
print("Processing transactions...")
txn = spark.read.csv(BRONZE + "/transactions.csv", header=True, inferSchema=True)
txn_v2 = spark.read.csv(BRONZE + "/transactions_v2.csv", header=True, inferSchema=True)
txn_all = txn.unionByName(txn_v2) \
    .withColumn("transaction_date", F.to_date(F.col("transaction_date").cast("string"), "yyyyMMdd")) \
    .withColumn("membership_expire_date", F.to_date(F.col("membership_expire_date").cast("string"), "yyyyMMdd")) \
    .dropDuplicates()
txn_all.write.mode("overwrite").parquet(SILVER + "/transactions")
print("transactions done")

# train labels
print("Processing train labels...")
train = spark.read.csv(BRONZE + "/train.csv", header=True, inferSchema=True)
train_v2 = spark.read.csv(BRONZE + "/train_v2.csv", header=True, inferSchema=True)
train_all = train.unionByName(train_v2).dropDuplicates(["msno"])
train_all.write.mode("overwrite").parquet(SILVER + "/train")
print("train done")

# user_logs
print("Processing user_logs (large, ~30 min)...")
logs = spark.read.csv(BRONZE + "/user_logs.csv", header=True, inferSchema=True)
logs_v2 = spark.read.csv(BRONZE + "/user_logs_v2.csv", header=True, inferSchema=True)
logs_all = logs.unionByName(logs_v2) \
    .withColumn("date", F.to_date(F.col("date").cast("string"), "yyyyMMdd")) \
    .dropDuplicates()
logs_all.write.mode("overwrite").partitionBy("date").parquet(SILVER + "/user_logs")
print("user_logs done")

print("=== Bronze to Silver complete! ===")
spark.stop()
