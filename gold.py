# Databricks notebook source
# =========================================
# 1. SETUP
# =========================================
from pyspark.sql.functions import *

spark.sql("USE CATALOG lending360new")

SILVER = "lending360new.silver"
GOLD = "lending360new.gold"

# =========================================
# 2. HELPER FUNCTIONS
# =========================================

def get_col(df, names):
    for c in names:
        if c in df.columns:
            return c
    return None

# =========================================
# 3. SAFE TRANSFORMATIONS
# =========================================
def apply_safe_transformations(df):

    cols = df.columns

    amount_col = get_col(df, ["loan_amount_usd", "loan_amount", "amount", "sanction_amount"])
    exchange_col = get_col(df, ["exchange_rate"])

    if amount_col and exchange_col:
        df = df.withColumn(
            "loan_amount_inr",
            col(amount_col).cast("double") * col(exchange_col).cast("double")
        )

    if "interest_rate_bps" in cols:
        df = df.withColumn("interest_rate_pct", col("interest_rate_bps") / 100)

    if "tenure_years" in cols:
        df = df.withColumn("tenure_months", col("tenure_years") * 12)

    if "due_date" in cols:
        df = df.withColumn(
            "is_overdue",
            when(col("due_date") < current_date(), 1).otherwise(0)
        )

    return df

# =========================================
# 4. LOAD LOOKUPS
# =========================================

borrower_df = spark.table(f"{SILVER}.borrower_profile_cleaned") \
    .select("borrower_id", col("full_name").alias("borrower_name")) \
    .dropDuplicates()

officer_df = spark.table(f"{SILVER}.loan_officers_cleaned") \
    .select("officer_id", col("full_name").alias("officer_name")) \
    .dropDuplicates()

branch_df_full = spark.table(f"{SILVER}.branch_master_dirty_cleaned").dropDuplicates()

# ✅ Aggregation with safe column names
repayment_df = spark.table(f"{SILVER}.repayment_summary_cleaned") \
    .groupBy("loan_id") \
    .agg(
        count("*").alias("repayment_count_agg"),
        sum("amount_in_inr").alias("total_repaid")
    )

# =========================================
# 5. SAFE ENRICHMENT (FINAL VERSION)
# =========================================
def apply_safe_enrichment(df, table_name):

    # Borrower
    if "borrower_id" in df.columns:
        df = df.join(borrower_df, "borrower_id", "left")

    # Officer
    if "officer_id" in df.columns:
        df = df.join(officer_df, "officer_id", "left")

    # Repayment (skip self join)
    if table_name != "repayment_summary_cleaned" and "loan_id" in df.columns:
        df = df.join(repayment_df, "loan_id", "left")

    # Branch (dynamic safe join)
    if "branch_id" in df.columns:

        missing_cols = [
            c for c in branch_df_full.columns
            if c not in df.columns and c != "branch_id"
        ]

        if missing_cols:
            df = df.join(
                branch_df_full.select(["branch_id"] + missing_cols),
                "branch_id",
                "left"
            )

    # Safe fill
    fill_dict = {}

    if "repayment_count_agg" in df.columns:
        fill_dict["repayment_count_agg"] = 0

    if "total_repaid" in df.columns:
        fill_dict["total_repaid"] = 0

    if fill_dict:
        df = df.fillna(fill_dict)

    return df

# =========================================
# 6. PROCESS FUNCTION
# =========================================
def process_table(table):

    try:
        print(f"🔄 Processing: {table}")

        df = spark.table(f"{SILVER}.{table}")

        df = apply_safe_transformations(df)
        df = apply_safe_enrichment(df, table)

        df = df.withColumn("processing_date", current_date())

        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{GOLD}.{table}_gold")

        print(f"✅ Success: {table} | Rows: {df.count()}")

    except Exception as e:
        print(f"❌ Error: {table} → {str(e)}")

# =========================================
# 7. AUTO LOAD ALL CLEANED TABLES
# =========================================
tables = [
    row.tableName for row in spark.sql(f"SHOW TABLES IN {SILVER}").collect()
    if row.tableName.endswith("_cleaned")
]

for t in tables:
    process_table(t)

# =========================================
# 8. VERIFY
# =========================================
display(spark.sql(f"SHOW TABLES IN {GOLD}"))

# COMMAND ----------

