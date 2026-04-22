"""
Spark Job: Silver → Gold
Feature aggregation per user, join all tables.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("KKBox Silver to Gold") \
    .getOrCreate()

BUCKET = "gs://kkbox-churn-prediction-493716-data"
SILVER = BUCKET + "/silver"
GOLD = BUCKET + "/gold"

print("Loading silver tables...")
members = spark.read.parquet(SILVER + "/members")
transactions = spark.read.parquet(SILVER + "/transactions")
train = spark.read.parquet(SILVER + "/train")
user_logs = spark.read.parquet(SILVER + "/user_logs")

print("Building transaction features...")
txn_features = transactions.groupBy("msno").agg(
    F.count("*").alias("total_transactions"),
    F.sum("actual_amount_paid").alias("total_amount_paid"),
    F.avg("actual_amount_paid").alias("avg_amount_paid"),
    F.max("membership_expire_date").alias("latest_expire_date"),
    F.sum("is_auto_renew").alias("auto_renew_count"),
    F.sum("is_cancel").alias("cancel_count"),
)

print("Building user log features...")
log_features = user_logs.groupBy("msno").agg(
    F.count("*").alias("total_log_days"),
    F.sum("total_secs").alias("total_secs"),
    F.avg("total_secs").alias("avg_daily_secs"),
    F.sum("num_25").alias("total_num_25"),
    F.sum("num_50").alias("total_num_50"),
    F.sum("num_75").alias("total_num_75"),
    F.sum("num_985").alias("total_num_985"),
    F.sum("num_100").alias("total_num_100"),
    F.sum("num_unq").alias("total_num_unq"),
)

print("Joining all features...")
gold = train \
    .join(members, on="msno", how="left") \
    .join(txn_features, on="msno", how="left") \
    .join(log_features, on="msno", how="left")

print("Writing to Gold layer...")
gold.write.mode("overwrite").parquet(GOLD + "/features")
print("=== Silver to Gold complete! ===")
spark.stop()
