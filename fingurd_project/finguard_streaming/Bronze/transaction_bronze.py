from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
import json

@dp.table(
    name = "finguard.bronze.transactions"
    ,comment="Transaction raw stream data ingested by kafka"
)

def transaction_bronze() -> DataFrame:
    kafka_connection_json = dbutils.secrets.get(scope="finguard-scope", key="kafka_connection_details")
    kafka_config = json.loads(kafka_connection_json)
    BOOTSTRAP_SERVERS=kafka_config['kafka_bootstrap_servers']
    API_KEY=kafka_config['kafka_api_key']
    API_SECRET=kafka_config['kafka_api_secret']
    TOPIC_NAME=kafka_config['kafka_topic']

    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'

    streaming_df = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
           .option("subscribe", TOPIC_NAME) 
           .option("kafka.security.protocol","SASL_SSL")
           .option("kafka.sasl.mechanism", "PLAIN")
           .option("kafka.sasl.jaas.config", jaas_config)
           .option("startingOffsets", "earliest")
           .load()
            )
    
    parsed_streaming_df = streaming_df.select(col("key").cast("string"), col("value").cast("string"), col("topic"), col("partition"),
                                               col("offset"), col("timestamp"), col("timestampType"),F.current_timestamp().alias("ingest_timestamp"))
    return parsed_streaming_df