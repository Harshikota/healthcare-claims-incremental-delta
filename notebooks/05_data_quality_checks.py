from pyspark.sql.functions import col

df = spark.read.format("delta").load("/mnt/gold/healthcare/gold_claims_latest")

null_claim_id = df.filter(col("claim_id").isNull()).count()
null_provider_id = df.filter(col("provider_id").isNull()).count()

dup_claims = (df.groupBy("claim_id")
              .count()
              .filter(col("count") > 1)
              .count())

neg_billed = df.filter(col("billed_amt") < 0).count()
neg_approved = df.filter(col("approved_amt") < 0).count()

invalid_status = df.filter(~col("status").isin("APPROVED", "REJECTED", "PENDING")).count()

print("===== Data Quality Checks =====")
print("Null claim_id:", null_claim_id)
print("Null provider_id:", null_provider_id)
print("Duplicate claim_id groups:", dup_claims)
print("Negative billed_amt rows:", neg_billed)
print("Negative approved_amt rows:", neg_approved)
print("Invalid status rows:", invalid_status)
print("================================")