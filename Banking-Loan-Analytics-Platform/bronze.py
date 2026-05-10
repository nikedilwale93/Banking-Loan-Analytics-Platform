# Databricks notebook source

from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.functions import when

# Read table
# df = spark.table("lending360new.bronze.repayment_summary")
# df = spark.table("lending360new.bronze.repayments")
df = spark.table("lending360new.bronze.loan_officers")


# df = df.withColumn(
#     "officer_id",
#     when(col("officer_id").rlike("^O[0-9]+$"),
#          regexp_replace(col("officer_id"), "^O", "0")
#     ).otherwise(col("officer_id"))
# )

# df = df.withColumn("officer_id", col("officer_id").cast("int"))

# Step 1: Replace 'O' with '0'
df = df.withColumn(
    "officer_id",
    regexp_replace(col("officer_id"), "O", "0")
)
# Step 2: Convert to integer
df = df.withColumn(
    "officer_id",
    col("officer_id").cast("int")
)

# Step 3: Overwrite existing table
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("lending360new.bronze.loan_officers")
    # .saveAsTable("lending360new.bronze.repayments")
    # .saveAsTable("lending360new.bronze.loan_officers")
df.show()





# COMMAND ----------

# COMMAND ---------- 1. SETUP

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import configparser

spark.sql("USE CATALOG lending360new")

BRONZE_SCHEMA = "lending360new.bronze"
SILVER_SCHEMA = "lending360new.silver"
REJECT_SCHEMA = "lending360new.reject"

# COMMAND ---------- 2. LOAD TABLES

config = configparser.ConfigParser()
config.read("/Volumes/lending360new/source1/volumn_csv/pipeline.config")

raw_tables = config.get("tables", "raw").split(",")
cleaned_tables = config.get("tables", "cleaned").split(",")

all_tables = [t.strip() for t in (raw_tables + cleaned_tables)]

# COMMAND ---------- 3. DATA QUALITY RULES

def apply_data_quality_rules(df, table_name):

    reject_df = None
    valid_df = df

    # -------------------------------
    # RULE 1: NULL CHECK
    # -------------------------------
    critical_cols = [c for c in df.columns if c.endswith("_id")]

    if critical_cols:
        cond = None
        for c in critical_cols:
            cond = col(c).isNull() if cond is None else (cond | col(c).isNull())

        tmp = valid_df.filter(cond)\
            .withColumn("reject_reason", lit("NULL_VALUES"))

        valid_df = valid_df.filter(~cond)
        reject_df = tmp if reject_df is None else reject_df.unionByName(tmp, True)

    # -------------------------------
    # RULE 2: REGEX
    # -------------------------------
    cond = None

    if "branch_id" in df.columns:
        c1 = ~col("branch_id").rlike("^BR[0-9]{3}$")
        cond = c1 if cond is None else (cond | c1)

    if "borrower_id" in df.columns:
        c2 = ~col("borrower_id").rlike("^B[0-9]{3,6}$")
        cond = c2 if cond is None else (cond | c2)

    if cond is not None:
        tmp = valid_df.filter(cond)\
            .withColumn("reject_reason", lit("INVALID_ID_FORMAT"))

        valid_df = valid_df.filter(~cond)
        reject_df = tmp if reject_df is None else reject_df.unionByName(tmp, True)

    # -------------------------------
    # RULE 3: GLOBAL NUMERIC VALIDATION
    # -------------------------------
    conditions = []

    if "age" in df.columns:
        conditions.append((col("age") < 18) | (col("age") > 100))

    if "income" in df.columns:
        df = df.withColumn("income", col("income").cast("double"))
        conditions.append((col("income") < 1000) | (col("income") > 1e7))

    if "credit_score" in df.columns:
        conditions.append((col("credit_score") < 300) | (col("credit_score") > 900))

    if "industry_code" in df.columns:
        conditions.append(length(col("industry_code")) > 6)

    # -------------------------------
    # RULE 4: TABLE-SPECIFIC (IMPORTANT FIX)
    # -------------------------------

    if table_name == "repayment_summary":

        if "amount_in_inr" in df.columns:
            df = df.withColumn("amount_in_inr", col("amount_in_inr").cast("double"))
            conditions.append((col("amount_in_inr") < 100) | (col("amount_in_inr") > 1e7))

        if "exchange_rate" in df.columns:
            conditions.append((col("exchange_rate") <= 0) | (col("exchange_rate") > 200))


    if table_name == "branch_master":

        if "employee_count" in df.columns:
            conditions.append((col("employee_count") < 1) | (col("employee_count") > 1000))

        if "loan_volume" in df.columns:
            df = df.withColumn("loan_volume", col("loan_volume").cast("double"))
            conditions.append((col("loan_volume") < 1000) | (col("loan_volume") > 1e9))


    # APPLY ALL CONDITIONS
    if conditions:
        cond = conditions[0]
        for c in conditions[1:]:
            cond = cond | c

        tmp = valid_df.filter(cond)\
            .withColumn("reject_reason", lit("OUTLIER_OR_INVALID_RANGE"))

        valid_df = valid_df.filter(~cond)
        reject_df = tmp if reject_df is None else reject_df.unionByName(tmp, True)

    # -------------------------------
    # RULE 5: DUPLICATES
    # -------------------------------
    valid_df = valid_df.dropDuplicates()

    # -------------------------------
    # EMPTY SAFE
    # -------------------------------
    if reject_df is None:
        reject_df = spark.createDataFrame([], df.schema)\
            .withColumn("reject_reason", lit(None))

    return valid_df, reject_df


# COMMAND ---------- 4. PROCESS FUNCTION

def process_table(table_name):

    try:
        print(f"🔄 Processing: {table_name}")

        df = spark.table(f"{BRONZE_SCHEMA}.{table_name}")

        df = df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

        for c, t in df.dtypes:
            if t == "string":
                df = df.withColumn(c, trim(col(c)))

        valid_df, reject_df = apply_data_quality_rules(df, table_name)

        # ✅ SILVER (_cleaned)
        valid_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{SILVER_SCHEMA}.{table_name}_cleaned")

        # ✅ REJECT
        reject_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{REJECT_SCHEMA}.{table_name}")

        print(f"✅ Success: {table_name}")

    except Exception as e:
        print(f"❌ Error in {table_name}: {str(e)}")


# COMMAND ---------- 5. RUN

for tbl in all_tables:
    process_table(tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC