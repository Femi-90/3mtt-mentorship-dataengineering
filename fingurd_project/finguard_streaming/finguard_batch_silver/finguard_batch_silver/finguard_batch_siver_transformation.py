from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="finguard.silver.customers",
    comment="Finguard batch silver table"
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def customers_silver() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.customers")

    return bronze_df.select(
        "customer_id",
        "first_name",
        "last_name",
        "gender",
        "age",
        "city",
        "state",
        "country",
        "annual_income",
        "customer_segment",
        F.to_date("account_open_date").alias("account_open_date"),
        "risk_score",
        "preferred_spending_min",
        "preferred_spending_max",
        "preferred_city",
        "preferred_country",
        "trusted_device_id",
        "card_number",
        "card_type",
         F.lit("loluolafemiwa@gmail.com").alias("email"),
        "transaction_limit",
        "update_timestamp",
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )




