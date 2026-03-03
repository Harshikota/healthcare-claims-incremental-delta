from pyspark.sql.functions import count, sum as fsum, avg, when, col

claims_latest = spark.read.format("delta").load("/mnt/gold/healthcare/gold_claims_latest")

# Provider KPIs per provider_id + service_date
kpis = (claims_latest
    .groupBy("provider_id", "service_date")
    .agg(
        count("*").alias("total_claims"),
        fsum("billed_amt").alias("total_billed_amt"),
        fsum("approved_amt").alias("total_approved_amt"),
        avg("approved_amt").alias("avg_approved_amt"),
        fsum(when(col("status") == "APPROVED", 1).otherwise(0)).alias("approved_claims")
    )
    .withColumn("approval_rate", col("approved_claims") / col("total_claims"))
)

kpis.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("service_date") \
    .save("/mnt/gold/healthcare/gold_provider_kpis_daily")

print("✅ gold_provider_kpis_daily created at /mnt/gold/healthcare/gold_provider_kpis_daily")