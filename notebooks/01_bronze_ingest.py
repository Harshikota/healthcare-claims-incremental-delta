from pyspark.sql.functions import current_timestamp, input_file_name

# In real projects ADF lands files daily into RAW_PATH partitions.
# For this repo, we read a single sample file.
claims_raw_path = "/mnt/raw/healthcare/claims.csv"

claims_df = spark.read.csv(claims_raw_path, header=True, inferSchema=True)

claims_bronze = (claims_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

claims_bronze.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/bronze/healthcare/claims")

print("✅ Bronze claims table created at /mnt/bronze/healthcare/claims")