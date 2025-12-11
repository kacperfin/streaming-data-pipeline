import json
import logging
import sys
from time import time

import redis
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PRICES, KAFKA_CONSUMER_GROUP_PRICES, REDIS_HOST, REDIS_PORT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('prices_consumer')

# Prometheus metrics
CONSUMER_E2E_LATENCY = Histogram(
    'consumer_e2e_latency_seconds',
    'End-to-end latency from Binance timestamp to consumer processing',
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
    labelnames=['consumer', 'symbol']
)

CONSUMER_SYSTEM_LATENCY = Histogram(
    'consumer_system_latency_seconds',
    'System latency: producer receive to consumer processing complete (pipeline overhead)',
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    labelnames=['consumer', 'symbol']
)

CONSUMER_PROCESSING_LATENCY = Histogram(
    'consumer_processing_latency_seconds',
    'Time to process message (deserialize + store in Redis)',
    buckets=(0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 5.0),
    labelnames=['consumer', 'symbol']
)

CONSUMER_MESSAGES_PROCESSED = Counter(
    'consumer_messages_processed_total',
    'Total number of messages processed by consumer',
    labelnames=['consumer', 'symbol']
)

CONSUMER_ERRORS = Counter(
    'consumer_errors_total',
    'Total number of errors encountered by consumer',
    labelnames=['consumer', 'error_type']
)

class PricesConsumer:
    """Consumer that reads price updates from Kafka and stores them in Redis."""

    def __init__(
        self,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC_PRICES,
        kafka_group_id=KAFKA_CONSUMER_GROUP_PRICES,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT
    ):
        """
        Initialize the Prices Consumer.

        Args:
            kafka_bootstrap_servers: Kafka broker address
            kafka_topic: Kafka topic to consume from
            kafka_group_id: Consumer group ID
            redis_host: Redis host address
            redis_port: Redis port
        """
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_group_id = kafka_group_id
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.consumer = None
        self.redis_client = None
        self.message_count = 0
        self.error_count = 0

        # Initialize connections
        self._init_redis()
        self._init_kafka_consumer()

    def _init_redis(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"Redis connection established: {self.redis_host}:{self.redis_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def _init_kafka_consumer(self):
        """Initialize Kafka consumer with proper configuration."""
        try:
            self.consumer = KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id=self.kafka_group_id,
                # Deserializers
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                # Consumer behavior
                auto_offset_reset='latest',  # Start from latest messages (don't replay history)
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,  # Commit offsets every second
                # Low-latency tuning: minimize wait time for fetching data
                fetch_min_bytes=1,  # Don't wait for batches, fetch immediately
                fetch_max_wait_ms=10,  # Wait max 10ms (default is 500ms - causes latency!)
                # Performance tuning
                max_poll_records=500,  # Process up to 500 messages per poll
                max_poll_interval_ms=300000,  # 5 minutes
                session_timeout_ms=10000,  # 10 seconds
                heartbeat_interval_ms=3000,  # 3 seconds
            )
            logger.info(f"Kafka consumer initialized. Topic: {self.kafka_topic}, Group: {self.kafka_group_id}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    def _store_price_in_redis(self, symbol, price_data, binance_timestamp):
        """
        Store the latest price for a symbol in Redis.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            price_data: Price data dictionary with price and timestamps
            binance_timestamp: Original timestamp from Binance (microseconds)
        """
        processing_start = time()

        try:
            # Store as JSON string with key: prices:{symbol}
            redis_key = f"price:{symbol}"
            self.redis_client.set(redis_key, json.dumps(price_data)) # type: ignore

            # Calculate end-to-end latency (AFTER Redis write completes)
            # This captures the full pipeline: Binance → Producer → Kafka → Consumer → Redis
            # Note: binance_timestamp is in microseconds from Binance API
            current_time_us = time() * 1_000_000  # Convert to microseconds for precision
            current_time_seconds = current_time_us / 1_000_000  # Also keep in seconds for consistency
            e2e_latency_seconds = (current_time_us - binance_timestamp) / 1_000_000
            CONSUMER_E2E_LATENCY.labels(consumer='prices', symbol=symbol).observe(e2e_latency_seconds)

            # Calculate system latency (pipeline overhead): Producer receive → Consumer done
            # This excludes network latency from Binance to Producer
            received_timestamp = price_data.get('received_timestamp')
            if received_timestamp:
                system_latency_seconds = (current_time_us - received_timestamp) / 1_000_000
                CONSUMER_SYSTEM_LATENCY.labels(consumer='prices', symbol=symbol).observe(system_latency_seconds)

            # Record processing latency (deserialization + Redis write)
            # Use the same timestamp as other metrics for consistency
            processing_latency = current_time_seconds - processing_start
            CONSUMER_PROCESSING_LATENCY.labels(consumer='prices', symbol=symbol).observe(processing_latency)

            # Record successful processing
            CONSUMER_MESSAGES_PROCESSED.labels(consumer='prices', symbol=symbol).inc()

            self.message_count += 1
            if self.message_count % 1000 == 0:
                logger.info(
                    f"Processed {self.message_count} messages. "
                    f"Latest: {symbol} = ${price_data['price']:.2f}, "
                    f"E2E latency: {e2e_latency_seconds*1000:.2f}ms"
                )

        except Exception as e:
            logger.error(f"Failed to store price in Redis for {symbol}: {e}")
            self.error_count += 1
            CONSUMER_ERRORS.labels(consumer='prices', error_type='redis_store').inc()

    def start(self):
        """Start consuming messages from Kafka and storing them in Redis."""
        try:
            logger.info(f"Starting to consume from topic '{self.kafka_topic}'...")
            logger.info("Waiting for messages...")

            # Consume messages indefinitely
            for message in self.consumer: # type: ignore
                try:
                    symbol = message.key
                    price_data = message.value

                    # Validate message structure
                    if not symbol or not isinstance(price_data, dict):
                        logger.warning(f"Invalid message format: key={symbol}, value={price_data}")
                        self.error_count += 1
                        CONSUMER_ERRORS.labels(consumer='prices', error_type='invalid_message').inc()
                        continue

                    # Extract binance timestamp
                    binance_timestamp = price_data.get('binance_timestamp')
                    if not binance_timestamp:
                        logger.warning(f"Missing binance_timestamp for {symbol}")
                        CONSUMER_ERRORS.labels(consumer='prices', error_type='missing_timestamp').inc()
                        continue

                    # Store in Redis
                    self._store_price_in_redis(symbol, price_data, binance_timestamp)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.error_count += 1
                    CONSUMER_ERRORS.labels(consumer='prices', error_type='processing').inc()

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        """Stop the consumer and cleanup resources."""
        logger.info("Stopping consumer...")

        # Close Kafka consumer
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

        # Close Redis connection
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")

        # Log statistics
        logger.info(
            f"Consumer stopped. Total messages processed: {self.message_count}, "
            f"Errors: {self.error_count}"
        )

def main():
    """Main entry point."""
    logger.info("Starting Prices Consumer (Kafka -> Redis)")
    logger.info(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Source Kafka topic: {KAFKA_TOPIC_PRICES}")
    logger.info(f"Consumer group: {KAFKA_CONSUMER_GROUP_PRICES}")
    logger.info(f"Target Redis keys: prices:{{symbol}}")

    # Start Prometheus metrics HTTP server
    metrics_port = 8001
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics available at http://localhost:{metrics_port}/metrics")

    try:
        # Create and start consumer (uses config defaults)
        consumer = PricesConsumer()
        consumer.start()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()