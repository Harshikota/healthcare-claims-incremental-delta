from pyspark.sql.functions import col, upper, trim, to_date, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

claims_bronze = spark.read.format("delta").load("/mnt/bronze/healthcare/claims")

# Standardize schema/types + basic cleanup
claims_clean = (claims_bronze
    .withColumn("claim_id", col("claim_id").cast("string"))
    .withColumn("member_id", col("member_id").cast("string"))
    .withColumn("provider_id", col("provider_id").cast("string"))
    .withColumn("service_date", to_date("service_date"))
    .withColumn("billed_amt", col("billed_amt").cast("double"))
    .withColumn("approved_amt", col("approved_amt").cast("double"))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("updated_at", to_timestamp("updated_at"))
)

# Dedup: keep latest row per claim_id by updated_at
w = Window.partitionBy("claim_id").orderBy(col("updated_at").desc())

claims_silver = (claims_clean
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

claims_silver.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/silver/healthcare/claims")

print("✅ Silver claims table created at /mnt/silver/healthcare/claims")