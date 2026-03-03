from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HealthcareClaimsPipeline").getOrCreate()

# Update if you use ABFSS paths instead of mounts
RAW_PATH = "/mnt/raw/healthcare"
BRONZE_PATH = "/mnt/bronze/healthcare"
SILVER_PATH = "/mnt/silver/healthcare"
GOLD_PATH = "/mnt/gold/healthcare"

print("Configured paths:")
print("RAW   :", RAW_PATH)
print("BRONZE:", BRONZE_PATH)
print("SILVER:", SILVER_PATH)
print("GOLD  :", GOLD_PATH)