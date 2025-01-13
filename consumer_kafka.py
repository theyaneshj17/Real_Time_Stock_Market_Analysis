from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from datetime import datetime
import json
import logging
import sys
import time
from typing import Dict, Any
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RetryPolicy, ConstantReconnectionPolicy

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
            'cassandra_host': '127.0.0.1',
            'cassandra_port': 9042,
            'cassandra_user': 'cassandra',
            'cassandra_password': 'cassandra',
            'keyspace': 'stock_market',
            'table': 'stock_prices',
            'kafka_topic': 'stockprice',
            'kafka_bootstrap_servers': ['localhost:29092']
        }
        
        self.session = None
        self.consumer = None
        self.cluster = None

    def setup_database_schema(self) -> None:
        """Set up or update database schema"""
        try:
            # Drop the existing table if it exists
            self.session.execute(f"""
                DROP TABLE IF EXISTS {self.config['keyspace']}.{self.config['table']}
            """)
            logger.info("Dropped existing table if it existed")

            # Create the table with the new schema
            self.session.execute(f"""
                CREATE TABLE {self.config['table']} (
                    symbol text,
                    price double,
                    timestamp timestamp,
                    company text,
                    market text,
                    PRIMARY KEY ((symbol), timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
            """)
            logger.info("Created new table with updated schema")

        except Exception as e:
            logger.error(f"Failed to setup database schema: {str(e)}")
            raise

    def setup_cassandra(self) -> None:
        """Initialize Cassandra connection with retry logic"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to connect to Cassandra at {self.config['cassandra_host']}:{self.config['cassandra_port']} (Attempt {attempt + 1}/{max_retries})")
                
                profile = ExecutionProfile(
                    retry_policy=RetryPolicy(),
                    request_timeout=60
                )
                
                auth_provider = PlainTextAuthProvider(
                    username=self.config['cassandra_user'],
                    password=self.config['cassandra_password']
                )

                self.cluster = Cluster(
                    contact_points=[self.config['cassandra_host']],
                    port=self.config['cassandra_port'],
                    auth_provider=auth_provider,
                    execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                    reconnection_policy=ConstantReconnectionPolicy(delay=1.0, max_attempts=100),
                    protocol_version=4
                )

                self.session = self.cluster.connect()
                
                # Create keyspace if it doesn't exist
                self.session.execute(f"""
                    CREATE KEYSPACE IF NOT EXISTS {self.config['keyspace']}
                    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
                """)
                
                self.session.set_keyspace(self.config['keyspace'])
                
                # Setup the database schema
                self.setup_database_schema()
                
                logger.info("Cassandra connection and schema setup completed successfully")
                return
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to Cassandra.")
                    raise

    def setup_kafka_consumer(self) -> None:
        """Initialize Kafka consumer with error handling"""
        try:
            self.consumer = KafkaConsumer(
                self.config['kafka_topic'],
                bootstrap_servers=self.config['kafka_bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='stock_price_consumer_group'
            )
            logger.info("Kafka consumer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to setup Kafka consumer: {str(e)}")
            raise

    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message and insert into Cassandra"""
        try:
            current_timestamp = datetime.utcnow()
            
            insert_query = SimpleStatement(f"""
                INSERT INTO {self.config['table']} 
                (symbol, price, timestamp, company, market)
                VALUES (%s, %s, %s, %s, %s)
            """)
            
            self.session.execute(
                insert_query,
                (
                    message['symbol'],
                    float(message['price']),
                    current_timestamp,
                    message.get('company', message['symbol']),
                    message.get('market', 'UNKNOWN')
                )
            )
            logger.info(f"Successfully inserted data for symbol: {message['symbol']} at {current_timestamp}")
            
        except Exception as e:
            logger.error(f"Failed to process message: {str(e)}")
            logger.error(f"Message content: {message}")
            raise

    def run(self) -> None:
        """Main processing loop"""
        try:
            self.setup_cassandra()
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
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("Cleanup completed")

if __name__ == "__main__":
    consumer = StockPriceConsumer()
    consumer.run()