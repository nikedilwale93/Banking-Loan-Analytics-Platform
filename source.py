# Databricks notebook source
# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType,DatetimeType, DateType

def ingest_csv(path: str, table_name: str, schema: StructType):
    df = (spark.read
          .option("header", True)
          .schema(schema)
          .csv(path))
    
    df.show()
    df.write.format("delta").mode("overwrite").saveAsTable(f"lending360new.bronze.{table_name}")
    print(f" Ingested CSV file into {table_name}")
    return df

# Define schema
csv_schema_1 = StructType([
    StructField("borrower_id", StringType()),
    StructField("full_name", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("location", StringType()),
    StructField("marital_status", StringType()),
    StructField("employment_type", StringType()),
    StructField("income", DoubleType()),
    StructField("industry_code", StringType()),
    StructField("credit_score", IntegerType()),
    StructField("default_flag", BooleanType())
])

csv_schema_2 = StructType([
    StructField("branch_id", StringType()),
    StructField("branch_name", StringType()),
    StructField("branch_location", StringType()),
    StructField("region", StringType()),
    StructField("manager_name", StringType()),
    StructField("opened_date", DateType()),
    StructField("employee_count", IntegerType()),
    StructField("loan_volume", DoubleType()),
    StructField("compliance_rating", StringType()),
    StructField("active_flag", BooleanType())
])


ingest_csv("/Volumes/lending360new/source1/volumn_csv/customer_metadata_dirty.csv", "customer_metadata", csv_schema_1)
ingest_csv("/Volumes/lending360new/source1/volumn_csv/branch_master_dirty.csv", "branch_master_dirty", csv_schema_2)



# COMMAND ----------

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType,DatetimeType, DateType

from pyspark.sql import Row

def ingest_json(path: str, table_name: str, schema: StructType):
    df = spark.read.schema(schema).json(path)

    df.show()

    df.write.format("delta").mode("overwrite").saveAsTable(f"lending360new.bronze.{table_name}")
    print(f"✅ Ingested JSON file into {table_name}")

# Define schema using case class style
json_schema_1 = StructType([
    StructField("loan_id", StringType()),
    StructField("borrower_id", StringType()),
    StructField("principal", DoubleType()),
    StructField("interest_rate_bps", IntegerType()),
    StructField("tenure_years", IntegerType()),
    StructField("issue_date_str", StringType()),
    StructField("loan_purpose", StringType()),
    StructField("collateral_id", StringType()),
    StructField("branch_id", StringType()),
    StructField("industry_code", StringType()),
    StructField("credit_score", IntegerType())
])

json_schema_2 = StructType([
    StructField("collateral_id", StringType()),
    StructField("collateral_type", StringType()),
    StructField("valuation", DoubleType()),
    StructField("valuation_date", StringType()),
    StructField("location", StringType()),
    StructField("ownership_verified", BooleanType()),
    StructField("risk_grade", StringType()),
    StructField("insurance_flag", BooleanType()),
    StructField("insurance_expiry", StringType()),
    StructField("asset_age_years", IntegerType()),
    StructField("asset_condition", StringType())
])

# Example usage
ingest_json("/Volumes/lending360new/source1/adls_json/loan_application.json", "loan_applications", json_schema_1)
ingest_json("/Volumes/lending360new/source1/adls_json/loan_collateral.json","loan_collateral", json_schema_2)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lending360new;
# MAGIC USE SCHEMA bronze;
# MAGIC CREATE TABLE credit_bureau (
# MAGIC   bureau_id BIGINT,
# MAGIC   borrower_id STRING,
# MAGIC   credit_score INT,
# MAGIC   score_date DATE,
# MAGIC   score_band STRING,
# MAGIC   default_flag BOOLEAN,
# MAGIC   last_default_date DATE,
# MAGIC   total_defaults INT,
# MAGIC   inquiries INT,
# MAGIC   active_loans INT,
# MAGIC   closed_loans INT,
# MAGIC   avg_utilization DOUBLE,
# MAGIC   max_utilization DOUBLE,
# MAGIC   bureau_name STRING,
# MAGIC   rating_agency STRING,
# MAGIC   risk_grade STRING,
# MAGIC   remarks STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO credit_bureau (
# MAGIC   bureau_id, borrower_id, credit_score, score_date, score_band, default_flag, last_default_date,
# MAGIC   total_defaults, inquiries, active_loans, closed_loans, avg_utilization, max_utilization,
# MAGIC   bureau_name, rating_agency, risk_grade, remarks, created_at, updated_at
# MAGIC ) VALUES
# MAGIC (1, 'B1001', 720, '2025-08-01', 'Excellent', false, NULL, 0, 2, 3, 5, 35.5, 60.0, 'CIBIL', 'CRISIL', 'A', 'Stable borrower', '2025-08-01 10:00:00', '2025-08-01 10:00:00'),
# MAGIC (2, 'B1002', 680, '2025-08-01', 'Good', false, NULL, 1, 3, 2, 4, 42.0, 70.0, 'Experian', 'ICRA', 'B', 'Moderate risk', '2025-08-01 10:05:00', '2025-08-01 10:05:00'),
# MAGIC (3, 'B1003', 750, '2025-08-01', 'Excellent', false, NULL, 0, 1, 4, 6, 28.0, 55.0, 'Equifax', 'CARE', 'A', 'Low exposure', '2025-08-01 10:10:00', '2025-08-01 10:10:00'),
# MAGIC (4, 'B1004', 640, '2025-08-01', 'Fair', true, '2024-12-15', 2, 5, 1, 3, 65.0, 85.0, 'CIBIL', 'CRISIL', 'C', 'Recent default', '2025-08-01 10:15:00', '2025-08-01 10:15:00'),
# MAGIC (5, 'B1005', 710, '2025-08-01', 'Good', false, NULL, 0, 2, 3, 4, 38.0, 62.0, 'Experian', 'ICRA', 'B', 'Consistent payments', '2025-08-01 10:20:00', '2025-08-01 10:20:00'),
# MAGIC (6, 'B1006', 690, '2025-08-01', 'Good', false, NULL, 1, 4, 2, 3, 45.0, 68.0, 'Equifax', 'CARE', 'B', 'Improving trend', '2025-08-01 10:25:00', '2025-08-01 10:25:00'),
# MAGIC (7, 'B1007', 730, '2025-08-01', 'Very Good', false, NULL, 0, 1, 3, 5, 30.0, 50.0, 'CIBIL', 'CRISIL', 'A', 'Strong repayment history', '2025-08-01 10:30:00', '2025-08-01 10:30:00'),
# MAGIC (8, 'B1008', 760, '2025-08-01', 'Excellent', false, NULL, 0, 2, 4, 6, 25.0, 48.0, 'Experian', 'ICRA', 'A', 'Top-tier borrower', '2025-08-01 10:35:00', '2025-08-01 10:35:00'),
# MAGIC (9, 'B1009', 670, '2025-08-01', 'Fair', true, '2025-03-10', 3, 6, 2, 2, 70.0, 90.0, 'Equifax', 'CARE', 'C', 'Multiple defaults', '2025-08-01 10:40:00', '2025-08-01 10:40:00'),
# MAGIC (10, 'B1010', 745, '2025-08-01', 'Very Good', false, NULL, 0, 2, 3, 4, 32.0, 58.0, 'CIBIL', 'CRISIL', 'A', 'Reliable client', '2025-08-01 10:45:00', '2025-08-01 10:45:00');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lending360new;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE TABLE borrower_profile (
# MAGIC   borrower_id STRING,
# MAGIC   full_name STRING,
# MAGIC   gender STRING,
# MAGIC   age INT,
# MAGIC   location STRING,
# MAGIC   marital_status STRING,
# MAGIC   employment_type STRING,
# MAGIC   income DOUBLE,
# MAGIC   industry_code STRING,
# MAGIC   sector STRING,
# MAGIC   credit_score INT,
# MAGIC   default_flag BOOLEAN,
# MAGIC   fraud_flag BOOLEAN,
# MAGIC   total_exposure DOUBLE,
# MAGIC   loan_count INT,
# MAGIC   avg_interest_rate DOUBLE,
# MAGIC   purpose_tag STRING,
# MAGIC   risk_score INT,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC INSERT INTO borrower_profile VALUES
# MAGIC ('B1001', 'Ravi Kumar', 'Male', 42, 'Mumbai', 'Married', 'Salaried', 1200000, '6201', 'Information Technology', 720, false, false, 500000, 1, 6.5, 'Machinery', 1, '2025-08-01 10:00:00', '2025-08-01 10:00:00'),
# MAGIC ('B1002', 'Neha Sharma', 'Female', 35, 'Pune', 'Single', 'Self-Employed', 950000, '4100', 'Logistics', 680, false, false, 750000, 1, 7.2, 'Expansion', 2, '2025-08-01 10:05:00', '2025-08-01 10:05:00'),
# MAGIC ('B1003', 'Amit Joshi', 'Male', 50, 'Delhi', 'Married', 'Salaried', 1500000, '1100', 'Manufacturing', 750, false, false, 1200000, 2, 6.0, 'Infrastructure', 1, '2025-08-01 10:10:00', '2025-08-01 10:10:00'),
# MAGIC ('B1004', 'Sunita Rao', 'Female', 29, 'Chennai', 'Single', 'Self-Employed', 800000, '6201', 'Information Technology', 640, true, true, 300000, 1, 8.0, 'Furniture', 3, '2025-08-01 10:15:00', '2025-08-01 10:15:00'),
# MAGIC ('B1005', 'Manoj Patil', 'Male', 45, 'Hyderabad', 'Married', 'Salaried', 1300000, '2100', 'Construction', 710, false, false, 950000, 1, 7.0, 'Acquisition', 2, '2025-08-01 10:20:00', '2025-08-01 10:20:00'),
# MAGIC ('B1006', 'Priya Desai', 'Female', 38, 'Ahmedabad', 'Married', 'Salaried', 1100000, '5100', 'Retail', 690, false, false, 650000, 1, 6.7, 'Payroll', 2, '2025-08-01 10:25:00', '2025-08-01 10:25:00'),
# MAGIC ('B1007', 'Rakesh Mehta', 'Male', 41, 'Kolkata', 'Married', 'Self-Employed', 1000000, '6201', 'Information Technology', 730, false, false, 400000, 1, 6.2, 'Machinery', 1, '2025-08-01 10:30:00', '2025-08-01 10:30:00'),
# MAGIC ('B1008', 'Anjali Verma', 'Female', 33, 'Bangalore', 'Single', 'Salaried', 1250000, '3100', 'Real Estate', 760, false, false, 850000, 1, 6.9, 'Infrastructure', 1, '2025-08-01 10:35:00', '2025-08-01 10:35:00'),
# MAGIC ('B1009', 'Suresh Nair', 'Male', 47, 'Jaipur', 'Married', 'Salaried', 900000, '4100', 'Logistics', 670, true, true, 550000, 1, 7.1, 'Logistics', 2, '2025-08-01 10:40:00', '2025-08-01 10:40:00'),
# MAGIC ('B1010', 'Meena Iyer', 'Female', 36, 'Nagpur', 'Single', 'Self-Employed', 850000, '1100', 'Manufacturing', 745, false, false, 1000000, 1, 6.4, 'Equipment', 1, '2025-08-01 10:45:00', '2025-08-01 10:45:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG lending360new;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE TABLE loan_history (
# MAGIC   loan_id STRING,
# MAGIC   borrower_id STRING,
# MAGIC   principal DOUBLE,
# MAGIC   interest_rate_pct DOUBLE,
# MAGIC   tenure_months INT,
# MAGIC   emi DOUBLE,
# MAGIC   issue_date DATE,
# MAGIC   due_date DATE,
# MAGIC   loan_type STRING,
# MAGIC   risk_score INT,
# MAGIC   is_overdue BOOLEAN,
# MAGIC   branch_id STRING,
# MAGIC   credit_score INT,
# MAGIC   default_flag BOOLEAN,
# MAGIC   collateral_id STRING,
# MAGIC   valuation DOUBLE,
# MAGIC   compliance_status STRING,
# MAGIC   purpose_tag STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC
# MAGIC INSERT INTO loan_history VALUES
# MAGIC ('L1001', 'B1001', 500000, 6.5, 60, 9755.00, '2020-08-01', '2025-08-01', 'Working Capital', 1, false, 'BR101', 720, false, 'C1001', 1200000, 'Compliant', 'Machinery', '2025-08-01 10:00:00', '2025-08-01 10:00:00'),
# MAGIC ('L1002', 'B1002', 750000, 7.2, 36, 23150.00, '2022-07-15', '2025-07-15', 'General', 2, true, 'BR102', 680, false, 'C1002', 850000, 'Review Required', 'Expansion', '2025-08-01 10:05:00', '2025-08-01 10:05:00'),
# MAGIC ('L1003', 'B1003', 1200000, 6.0, 84, 17450.00, '2018-06-20', '2025-06-20', 'Construction Loan', 1, false, 'BR103', 750, false, 'C1003', 1600000, 'Compliant', 'Infrastructure', '2025-08-01 10:10:00', '2025-08-01 10:10:00'),
# MAGIC ('L1004', 'B1004', 300000, 8.0, 24, 13500.00, '2023-05-10', '2025-05-10', 'General', 3, true, 'BR104', 640, true, 'C1004', 400000, 'Review Required', 'Furniture', '2025-08-01 10:15:00', '2025-08-01 10:15:00'),
# MAGIC ('L1005', 'B1005', 950000, 7.0, 48, 22800.00, '2021-04-01', '2025-04-01', 'Bridge Loan', 2, false, 'BR105', 710, false, 'C1005', 1100000, 'Compliant', 'Acquisition', '2025-08-01 10:20:00', '2025-08-01 10:20:00'),
# MAGIC ('L1006', 'B1006', 650000, 6.7, 72, 11200.00, '2019-03-12', '2025-03-12', 'Working Capital', 2, false, 'BR106', 690, false, 'C1006', 900000, 'Compliant', 'Payroll', '2025-08-01 10:25:00', '2025-08-01 10:25:00'),
# MAGIC ('L1007', 'B1007', 400000, 6.2, 36, 12250.00, '2022-02-25', '2025-02-25', 'Working Capital', 1, false, 'BR107', 730, false, 'C1007', 600000, 'Compliant', 'Machinery', '2025-08-01 10:30:00', '2025-08-01 10:30:00'),
# MAGIC ('L1008', 'B1008', 850000, 6.9, 60, 16600.00, '2020-01-30', '2025-01-30', 'Construction Loan', 1, false, 'BR108', 760, false, 'C1008', 1400000, 'Compliant', 'Infrastructure', '2025-08-01 10:35:00', '2025-08-01 10:35:00'),
# MAGIC ('L1009', 'B1009', 550000, 7.1, 48, 13200.00, '2021-01-10', '2025-01-10', 'Working Capital', 2, true, 'BR109', 670, true, 'C1009', 750000, 'Review Required', 'Logistics', '2025-08-01 10:40:00', '2025-08-01 10:40:00'),
# MAGIC ('L1010', 'B1010', 1000000, 6.4, 72, 17500.00, '2019-12-20', '2025-12-20', 'General', 1, false, 'BR110', 745, false, 'C1010', 1300000, 'Compliant', 'Equipment', '2025-08-01 10:45:00', '2025-08-01 10:45:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG lending360new;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE TABLE repayment_summary (
# MAGIC   loan_id STRING,
# MAGIC   borrower_id STRING,
# MAGIC   repayment_count INT,
# MAGIC   missed_payments INT,
# MAGIC   total_paid DOUBLE,
# MAGIC   avg_payment DOUBLE,
# MAGIC   last_payment_date DATE,
# MAGIC   late_fee_total DOUBLE,
# MAGIC   payment_mode_dominant STRING,
# MAGIC   status_flag STRING,
# MAGIC   officer_id STRING,
# MAGIC   branch_id STRING,
# MAGIC   currency STRING,
# MAGIC   exchange_rate DOUBLE,
# MAGIC   amount_in_inr DOUBLE,
# MAGIC   fiscal_year STRING,
# MAGIC   quarter STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC
# MAGIC
# MAGIC INSERT INTO repayment_summary VALUES
# MAGIC ('L1001', 'B1001', 48, 2, 480000.00, 10000.00, '2025-08-01', 1500.00, 'Online', 'Active', 'O101', 'BR101', 'USD', 83.2, 39936000.00, '2025-26', 'Q2', '2025-08-01 10:00:00', '2025-08-01 10:00:00'),
# MAGIC ('L1002', 'B1002', 36, 0, 342000.00, 9500.00, '2025-08-06', 0.00, 'Cheque', 'Active', 'O102', 'BR102', 'INR', 1.0, 342000.00, '2025-26', 'Q2', '2025-08-01 10:05:00', '2025-08-01 10:05:00'),
# MAGIC ('L1003', 'B1003', 60, 1, 720000.00, 12000.00, '2025-08-07', 250.00, 'Online', 'Active', 'O103', 'BR103', 'USD', 83.2, 59840000.00, '2025-26', 'Q2', '2025-08-01 10:10:00', '2025-08-01 10:10:00'),
# MAGIC ('L1004', 'B1004', 24, 3, 324000.00, 13500.00, '2025-08-08', 900.00, 'Cash', 'Delinquent', 'O104', 'BR104', 'INR', 1.0, 324000.00, '2025-26', 'Q2', '2025-08-01 10:15:00', '2025-08-01 10:15:00'),
# MAGIC ('L1005', 'B1005', 48, 0, 1094400.00, 22800.00, '2025-08-09', 0.00, 'Online', 'Active', 'O105', 'BR105', 'USD', 83.2, 91047680.00, '2025-26', 'Q2', '2025-08-01 10:20:00', '2025-08-01 10:20:00'),
# MAGIC ('L1006', 'B1006', 72, 2, 806400.00, 11200.00, '2025-08-10', 600.00, 'Cheque', 'Active', 'O106', 'BR106', 'INR', 1.0, 806400.00, '2025-26', 'Q2', '2025-08-01 10:25:00', '2025-08-01 10:25:00'),
# MAGIC ('L1007', 'B1007', 36, 1, 441000.00, 12250.00, '2025-08-11', 300.00, 'Online', 'Active', 'O107', 'BR107', 'USD', 83.2, 36631200.00, '2025-26', 'Q2', '2025-08-01 10:30:00', '2025-08-01 10:30:00'),
# MAGIC ('L1008', 'B1008', 60, 0, 996000.00, 16600.00, '2025-08-12', 0.00, 'Cash', 'Active', 'O108', 'BR108', 'INR', 1.0, 996000.00, '2025-26', 'Q2', '2025-08-01 10:35:00', '2025-08-01 10:35:00'),
# MAGIC ('L1009', 'B1009', 48, 4, 633600.00, 13200.00, '2025-08-13', 1200.00, 'Online', 'Delinquent', 'O109', 'BR109', 'USD', 83.2, 52619520.00, '2025-26', 'Q2', '2025-08-01 10:40:00', '2025-08-01 10:40:00'),
# MAGIC ('L1010', 'B1010', 72, 0, 1260000.00, 17500.00, '2025-08-14', 0.00, 'Cheque', 'Active', 'O110', 'BR110', 'INR', 1.0, 1260000.00, '2025-26', 'Q2', '2025-08-01 10:45:00', '2025-08-01 10:45:00');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lending360new;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE TABLE loan_officers (
# MAGIC   officer_id STRING PRIMARY KEY,
# MAGIC   full_name STRING,
# MAGIC   department STRING,
# MAGIC   designation STRING,
# MAGIC   branch_id STRING,
# MAGIC   joining_date DATE,
# MAGIC   experience_years INT,
# MAGIC   active_flag BOOLEAN,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   region STRING,
# MAGIC   total_loans INT,
# MAGIC   avg_loan_size DOUBLE ,
# MAGIC   compliance_score DOUBLE ,
# MAGIC   last_review_date DATE,
# MAGIC   review_rating STRING,
# MAGIC   fraud_cases INT,
# MAGIC   resolved_cases INT,
# MAGIC   escalation_flag BOOLEAN,
# MAGIC   remarks STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO loan_officers VALUES
# MAGIC ('O102', 'Alice Smith', 'Loan', 'Senior Officer', 'BR102', '2019-03-15', 5, true,
# MAGIC  'alice.smith@example.com', '9123456780', 'South', 80, 3500000.00, 90.5,
# MAGIC  '2023-02-10', 'Very Good', 1, 1, false, 'Consistent performer'),
# MAGIC
# MAGIC ('O103', 'Robert Brown', 'Loan', 'Officer', 'BR103', '2020-06-20', 3, true,
# MAGIC  'robert.brown@example.com', '9234567810', 'East', 60, 2500000.00, 88.0,
# MAGIC  '2023-03-05', 'Good', 2, 2, false, 'Needs improvement'),
# MAGIC
# MAGIC ('O104', 'Emily Davis', 'Loan', 'Manager', 'BR104', '2017-09-10', 8, true,
# MAGIC  'emily.davis@example.com', '9345678123', 'West', 120, 6000000.00, 96.5,
# MAGIC  '2023-01-20', 'Excellent', 0, 0, false, 'Top performer'),
# MAGIC
# MAGIC ('O105', 'Michael Johnson', 'Loan', 'Officer', 'BR105', '2021-01-25', 2, true,
# MAGIC  'michael.johnson@example.com', '9456781234', 'North', 40, 1500000.00, 85.0,
# MAGIC  '2023-04-01', 'Average', 3, 2, true, 'Requires monitoring'),
# MAGIC
# MAGIC ('O106', 'Sophia Lee', 'Loan', 'Senior Officer', 'BR106', '2018-11-11', 6, true,
# MAGIC  'sophia.lee@example.com', '9567812345', 'South', 90, 4200000.00, 92.3,
# MAGIC  '2023-02-18', 'Very Good', 1, 1, false, 'Reliable'),
# MAGIC
# MAGIC ('O107', 'David Wilson', 'Loan', 'Officer', 'BR107', '2022-02-14', 1, true,
# MAGIC  'david.wilson@example.com', '9678123456', 'East', 30, 1200000.00, 80.0,
# MAGIC  '2023-05-01', 'Average', 4, 3, true, 'Under training'),
# MAGIC
# MAGIC ('O108', 'Olivia Martinez', 'Loan', 'Manager', 'BR108', '2016-07-07', 9, true,
# MAGIC  'olivia.martinez@example.com', '9781234567', 'West', 150, 7000000.00, 97.8,
# MAGIC  '2023-01-05', 'Excellent', 0, 0, false, 'Outstanding'),
# MAGIC
# MAGIC ('O109', 'James Anderson', 'Loan', 'Senior Officer', 'BR109', '2019-12-01', 4, true,
# MAGIC  'james.anderson@example.com', '9892345678', 'North', 70, 3000000.00, 89.5,
# MAGIC  '2023-03-22', 'Good', 2, 2, false, 'Stable performance'),
# MAGIC
# MAGIC ('O110', 'Isabella Thomas', 'Loan', 'Officer', 'BR110', '2020-08-18', 3, true,
# MAGIC  'isabella.thomas@example.com', '9903456789', 'South', 55, 2000000.00, 87.2,
# MAGIC  '2023-04-10', 'Good', 1, 1, false, 'Improving'),
# MAGIC
# MAGIC ('O111', 'William Taylor', 'Loan', 'Manager', 'BR111', '2015-05-30', 10, true,
# MAGIC  'william.taylor@example.com', '9014567890', 'East', 180, 8000000.00, 98.5,
# MAGIC  '2023-01-12', 'Excellent', 0, 0, false, 'Highly experienced');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG lending360new;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE TABLE repayments (
# MAGIC   repayment_id BIGINT,
# MAGIC   loan_id STRING,
# MAGIC   borrower_id STRING,
# MAGIC   payment_date DATE,
# MAGIC   amount_paid DOUBLE ,
# MAGIC   status STRING,
# MAGIC   payment_mode STRING,
# MAGIC   late_fee DOUBLE ,
# MAGIC   missed_flag BOOLEAN,
# MAGIC   emi_number INT,
# MAGIC   remarks STRING,
# MAGIC   created_at TIMESTAMP,
# MAGIC   updated_at TIMESTAMP,
# MAGIC   branch_id STRING,
# MAGIC   officer_id STRING,
# MAGIC   currency STRING,
# MAGIC   exchange_rate DOUBLE ,
# MAGIC   amount_in_inr DOUBLE ,
# MAGIC   fiscal_year STRING,
# MAGIC   quarter STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO repayments (
# MAGIC   repayment_id, loan_id, borrower_id, payment_date, amount_paid, status, payment_mode, late_fee, missed_flag,
# MAGIC   emi_number, remarks, created_at, updated_at, branch_id, officer_id, currency, exchange_rate,
# MAGIC   amount_in_inr, fiscal_year, quarter
# MAGIC ) VALUES
# MAGIC (1, 'L1001', 'B1001', '2025-08-05', 10000.00, 'Paid', 'Online', 0.00, false, 1, 'On time', '2025-08-05 10:00:00', '2025-08-05 10:00:00', 'BR101', 'O101', 'USD', 83.2, 832000.00, '2025-26', 'Q2'),
# MAGIC (2, 'L1002', 'B1002', '2025-08-06', 9500.00, 'Paid', 'Cheque', 0.00, false, 2, 'Cleared', '2025-08-06 11:00:00', '2025-08-06 11:00:00', 'BR102', 'O102', 'INR', 1.0, 9500.00, '2025-26', 'Q2'),
# MAGIC (3, 'L1003', 'B1003', '2025-08-07', 12000.00, 'Missed', 'Online', 500.00, true, 3, 'Delayed', '2025-08-07 12:00:00', '2025-08-07 12:00:00', 'BR103', 'O103', 'USD', 83.2, 998400.00, '2025-26', 'Q2'),
# MAGIC (4, 'L1004', 'B1004', '2025-08-08', 8000.00, 'Paid', 'Cash', 0.00, false, 4, 'On time', '2025-08-08 13:00:00', '2025-08-08 13:00:00', 'BR104', 'O104', 'INR', 1.0, 8000.00, '2025-26', 'Q2'),
# MAGIC (5, 'L1005', 'B1005', '2025-08-09', 15000.00, 'Paid', 'Online', 0.00, false, 5, 'Processed', '2025-08-09 14:00:00', '2025-08-09 14:00:00', 'BR105', 'O105', 'USD', 83.2, 1248000.00, '2025-26', 'Q2'),
# MAGIC (6, 'L1006', 'B1006', '2025-08-10', 11000.00, 'Paid', 'Cheque', 0.00, false, 6, 'Cleared', '2025-08-10 15:00:00', '2025-08-10 15:00:00', 'BR106', 'O106', 'INR', 1.0, 11000.00, '2025-26', 'Q2'),
# MAGIC (7, 'L1007', 'B1007', '2025-08-11', 9500.00, 'Missed', 'Online', 300.00, true, 7, 'Late fee applied', '2025-08-11 16:00:00', '2025-08-11 16:00:00', 'BR107', 'O107', 'USD', 83.2, 790400.00, '2025-26', 'Q2'),
# MAGIC (8, 'L1008', 'B1008', '2025-08-12', 13000.00, 'Paid', 'Cash', 0.00, false, 8, 'On time', '2025-08-12 17:00:00', '2025-08-12 17:00:00', 'BR108', 'O108', 'INR', 1.0, 13000.00, '2025-26', 'Q2'),
# MAGIC (9, 'L1009', 'B1009', '2025-08-13', 10500.00, 'Paid', 'Online', 0.00, false, 9, 'Processed', '2025-08-13 18:00:00', '2025-08-13 18:00:00', 'BR109', 'O109', 'USD', 83.2, 873600.00, '2025-26', 'Q2'),
# MAGIC (10, 'L1010', 'B1010', '2025-08-14', 9800.00, 'Paid', 'Cheque', 0.00, false, 10, 'Cleared', '2025-08-14 19:00:00', '2025-08-14 19:00:00', 'BR110', 'O110', 'INR', 1.0, 9800.00, '2025-26', 'Q2');