import yfinance as yf
import json
import time
from time import sleep
from kafka import KafkaProducer
import sys
import six


# Function to fetch the latest stock price
def get_stock_data(ticker):
    stock_data = {}
    stock = yf.Ticker(ticker)
    stock_data["price"] = stock.fast_info.last_price  # Changed from "Price" to "price"
    stock_data["symbol"] = ticker  # Changed from "Name" to "symbol"
    stock_data["company"] = ticker  # Added company field
    stock_data["market"] = "NASDAQ"  # Added market field
    return stock_data

# Function to send stock data to Kafka
def send_to_kafka(producer, topic, stock_data):
    producer.send(topic, stock_data)
    producer.flush()  # Ensure message is sent

# Function to initialize Kafka producer
def initialize_kafka_producer(brokers):
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

# Main function to generate stock data and send to Kafka
def generate_stock_data(ticker, kafka_brokers, kafka_topic, iterations, interval):
    # Initialize Kafka producer
    producer = initialize_kafka_producer(kafka_brokers)
    
    # Generate and send stock data in a loop
    counter = 0
    while True:
        # Fetch the latest stock data
        stock_data = get_stock_data(ticker)
        
        # Print the stock data 
        print(stock_data)
        
        # Send stock data to Kafka
        send_to_kafka(producer, kafka_topic, stock_data)
        
        # Wait for the next iteration
        sleep(interval)
        counter += 1
    
    # Close the Kafka producer connection
    producer.close()

# Main execution
if __name__ == '__main__':
    
    ticker = "AMZN"  # Set the ticker symbol for Amazon
    kafka_brokers = "localhost:29092"  # Kafka brokers
    kafka_topic = 'stockprice'  # Kafka topic to send the data
    iterations = 20  # Number of iterations (number of stock price fetches)
    interval = 12  # Time interval between iterations (in seconds)
    
    # Generate and send stock data to Kafka
    generate_stock_data(ticker, kafka_brokers, kafka_topic, iterations, interval)
