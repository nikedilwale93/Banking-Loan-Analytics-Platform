# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,trim, to_date,lower,regexp_replace
from pyspark.sql.types import * 
import configparser

# Load table names from application.conf
config = configparser.ConfigParser()
config_data = config.read("/Volumes/lending360/source_json/json/pipeline.config")
print("Config data loaded")
print(config_data)

raw_tables = config.get("tables", "raw").split(",")

#cleaned_tables = config.get("tables", "cleaned").split(",")

all_tables_names = raw_tables #+ cleaned_tables

catalog_schema = "lending360.bronze"
all_tables = [f"{catalog_schema}.{tbl.strip()}" for tbl in all_tables_names]

print("Printing All Tables")
print(all_tables)

# COMMAND ----------

from pyspark.sql.functions import col
from functools import reduce
from pyspark.sql import functions as F

def split_valid_invalid_data(df):
    critical_fields = [c for c in ['loan_id', 'borrower_id','collateral_id','insurance_expiry'] if c in df.columns]
    
    if not critical_fields:
        print("No critical columns found")
        return df, spark.createDataFrame([], df.schema)

    # Condition: any column is NULL
    invalid_condition = reduce(lambda x, y: x | y, [col(c).isNull() for c in critical_fields])

    # Split data
    valid_df = df.filter(~invalid_condition)
    invalid_df = df.filter(invalid_condition)

    return valid_df, invalid_df

# COMMAND ----------

tables = [
    "lending360.bronze.loan_application_output",
    "lending360.bronze.loan_collateral_output" 
]

for tbl in tables:
    print(f"\n Processing: {tbl}")
    
    df = spark.table(tbl)
    valid_df, invalid_df = split_valid_invalid_data(df)

    print(" Valid Data")
    valid_df.show(truncate=False)

    print("Invalid Data")
    invalid_df.show(truncate=False)

    