import psycopg2
from psycopg2 import sql
from kafka import KafkaConsumer
from datetime import datetime
import json
import logging
import sys
import time
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class StockPriceConsumer:
    def __init__(self):
        self.config = {
            'postgres_host': 'localhost',
            'postgres_port': 5432,
            'postgres_user': 'postgres',
            'postgres_password': 'postgres',
            'postgres_db': 'stock_market',
            'table': 'stock_prices',
            'kafka_topic': 'stockprice',
            'kafka_bootstrap_servers': ['localhost:29092']  # Changed from localhost:29092 to kafka:9092
        }
        
        self.connection = None
        self.consumer = None

    def setup_database_schema(self) -> None:
        """Set up or update database schema in PostgreSQL"""
        try:
            cursor = self.connection.cursor()

            # Drop the existing table if it exists
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(self.config['table'])))
            logger.info("Dropped existing table if it existed")

            # Create the table with the new schema
            cursor.execute(f"""
                CREATE TABLE {self.config['table']} (
                    symbol VARCHAR(10),
                    price DOUBLE PRECISION,
                    timestamp TIMESTAMP,
                    company VARCHAR(100),
                    market VARCHAR(50),
                    PRIMARY KEY (symbol, timestamp)
                )
            """)
            self.connection.commit()
            logger.info("Created new table with updated schema")

        except Exception as e:
            logger.error(f"Failed to setup database schema: {str(e)}")
            raise

    def setup_postgresql(self) -> None:
        """Initialize PostgreSQL connection with retry logic"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to PostgreSQL at {self.config['postgres_host']}:{self.config['postgres_port']} (Attempt {attempt + 1}/{max_retries})")
                
                # Connect to PostgreSQL
                self.connection = psycopg2.connect(
                    host=self.config['postgres_host'],
                    port=self.config['postgres_port'],
                    user=self.config['postgres_user'],
                    password=self.config['postgres_password'],
                    dbname=self.config['postgres_db']
                )

                # Setup the database schema
                self.setup_database_schema()
                
                logger.info("PostgreSQL connection and schema setup completed successfully")
                return
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to PostgreSQL.")
                    raise

    def setup_kafka_consumer(self) -> None:
        """Initialize Kafka consumer with retry logic"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to Kafka at {self.config['kafka_bootstrap_servers']} (Attempt {attempt + 1}/{max_retries})")
                
                self.consumer = KafkaConsumer(
                    self.config['kafka_topic'],
                    bootstrap_servers=self.config['kafka_bootstrap_servers'],
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    group_id='stock_price_consumer_group'
                )
                
                logger.info("Kafka consumer initialized successfully")
                return
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to Kafka.")
                    raise

    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message and insert into PostgreSQL"""
        try:
            current_timestamp = datetime.utcnow()

            # Insert data into PostgreSQL table
            cursor = self.connection.cursor()
            insert_query = f"""
                INSERT INTO {self.config['table']} (symbol, price, timestamp, company, market)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                message['symbol'],
                float(message['price']),
                current_timestamp,
                message.get('company', message['symbol']),
                message.get('market', 'UNKNOWN')
            ))
            self.connection.commit()
            logger.info(f"Successfully inserted data for symbol: {message['symbol']} at {current_timestamp}")

        except Exception as e:
            logger.error(f"Failed to process message: {str(e)}")
            logger.error(f"Message content: {message}")
            raise

    def run(self) -> None:
        """Main processing loop"""
        try:
            self.setup_postgresql()
            self.setup_kafka_consumer()
            
            logger.info("Starting to consume messages...")
            for message in self.consumer:
                try:
                    self.process_message(message.value)
                    self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
                
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.cleanup()

    def cleanup(self) -> None:
        """Cleanup resources"""
        if self.consumer:
            self.consumer.close()
        if self.connection:
            self.connection.close()
        logger.info("Cleanup completed")

if __name__ == "__main__":
    consumer = StockPriceConsumer()
    consumer.run()