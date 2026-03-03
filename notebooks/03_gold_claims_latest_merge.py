from delta.tables import DeltaTable

source_df = spark.read.format("delta").load("/mnt/silver/healthcare/claims")

gold_path = "/mnt/gold/healthcare/gold_claims_latest"

# First run creates the Gold table
if not DeltaTable.isDeltaTable(spark, gold_path):
    (source_df
        .select("claim_id","member_id","provider_id","service_date",
                "billed_amt","approved_amt","status","updated_at")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("service_date")
        .save(gold_path)
    )
    print("✅ Created gold_claims_latest")
else:
    target = DeltaTable.forPath(spark, gold_path)

    (target.alias("t")
        .merge(source_df.alias("s"), "t.claim_id = s.claim_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("✅ MERGE completed into gold_claims_latest")