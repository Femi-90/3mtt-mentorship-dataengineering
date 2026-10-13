from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import ( 
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    TimestampType,
)
import json

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("card_number", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True),
])

@dp.table(
    name = "finguard.silver.transactions"
    ,comment="Parsed and cleaned transaction data"
)

@dp.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")
@dp.expect_or_drop("valid_amount","amount > 0")

def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.transactions")

    transformed_df = bronze_df.select(
        F.from_json(col("value"), transaction_schema).alias("parsed")
    ).select("parsed.*")

    return transformed_df
