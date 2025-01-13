from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from cassandra.cluster import Cluster
from datetime import datetime
import uuid

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaStructuredStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1," 
                "com.redislabs:spark-redis_2.12:3.1.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0") \
        .config("spark.redis.host", REDIS_HOST) \
        .config("spark.redis.port", REDIS_PORT) \
        .config("spark.redis.db", "0") \
        .config('spark.cassandra.connection.host', CASSANDRA_HOST) \
        .getOrCreate()

def read_kafka_stream(spark):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

def extract_data_from_kafka(kafka_stream):
    stock_schema = StructType([
        StructField("price", DoubleType(), True),
        StructField("symbol", StringType(), True),
        StructField("company", StringType(), True),
        StructField("market", StringType(), True),
        StructField("timestamp", TimestampType(), True)  # Add timestamp to track when data was ingested
    ])
    
    # Extract and parse the JSON from Kafka value
    stock_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), stock_schema).alias("data")) \
        .select("data.*")
    
    return stock_df

def agg_stock_data(stock_df):
    # Group the stock data by symbol and aggregate over a window of time, or simply for each stock symbol
    agg_df = stock_df.groupBy("symbol") \
        .agg(
            avg("price").alias("avg_price"),  # Calculate average price of stock
            max("price").alias("max_price"),  # Max price in the window or over time
            min("price").alias("min_price")   # Min price in the window or over time
        )
    return agg_df

def create_cassandra_connection():
    try:
        cluster = Cluster([CASSANDRA_HOST])
        return cluster.connect()
    except Exception as e:
        print(f"Could not create Cassandra connection due to {e}")
        return None

def create_keyspace(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
    """)
    print('Keyspace created')

def create_table(session):
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (
            id UUID PRIMARY KEY,
            symbol TEXT,
            company TEXT,
            market TEXT,
            price DOUBLE,
            timestamp TIMESTAMP
        )
    """)
    print("Table created successfully")

def write_to_redis(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.redis") \
        .option("table", "stock_data") \
        .option("key.column", "symbol") \
        .mode("append") \
        .save()

def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option('checkpointLocation', CHECKPOINT_LOCATION_CASSANDRA) \
        .option('keyspace', KEYSPACE) \
        .option('table', TABLE) \
        .mode("append") \
        .save()

def generate_uuid():
    return str(uuid.uuid4())

def main():
    # Create Spark session
    spark = create_spark_session()
    kafka_stream = read_kafka_stream(spark)
    
    # Extract the stock data from Kafka stream
    stock_df = extract_data_from_kafka(kafka_stream)
    
    # Add UUID to each row for uniqueness
    uuid_udf = udf(generate_uuid, StringType())
    stock_df = stock_df.withColumn("id", uuid_udf())

    # Create Cassandra connection and initialize keyspace and table
    session = create_cassandra_connection()
    if session:
        create_keyspace(session)
        create_table(session)
    
    # Aggregate stock data (average price, max, min)
    agg_df = agg_stock_data(stock_df)

    # Write the aggregate data to Redis and Cassandra
    query_redis = agg_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_redis) \
        .option("checkpointLocation", CHECKPOINT_LOCATION_SPECIFIC) \
        .start()

    query_cassandra = agg_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", CHECKPOINT_LOCATION_CASSANDRA) \
        .start()

    # Wait for termination of all queries
    query_redis.awaitTermination()
    query_cassandra.awaitTermination()

if __name__ == "__main__":
    # Configuration
    KAFKA_TOPIC_NAME = 'stockprice'  # Kafka topic for stock price data
    KAFKA_BOOTSTRAP_SERVER = 'localhost:29092'  # Kafka brokers
    REDIS_HOST = "localhost"
    REDIS_PORT = "6379"
    CASSANDRA_HOST = "localhost"
    KEYSPACE = "stock_streams"
    TABLE = "stock_prices"
    CHECKPOINT_LOCATION_CASSANDRA = "/tmp/check_point/cassandra"
    CHECKPOINT_LOCATION_SPECIFIC = "/tmp/check_point/specific_time"
    main()
