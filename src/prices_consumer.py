import json
import logging
import sys
import time

import redis
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PRICES, KAFKA_CONSUMER_GROUP_PRICES, REDIS_HOST, REDIS_PORT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('prices_consumer')

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

    def _store_price_in_redis(self, symbol, price_data):
        """
        Store the latest price for a symbol in Redis.

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            price_data: Price data dictionary with price and timestamps
        """
        try:
            # Add consumer processing timestamp
            price_data['consumer_timestamp'] = int(time.time() * 1_000_000)

            # Store as JSON string with key: prices:{symbol}
            redis_key = f"price:{symbol}"
            self.redis_client.set(redis_key, json.dumps(price_data)) # type: ignore

            self.message_count += 1
            if self.message_count % 100 == 0:
                logger.info(
                    f"Processed {self.message_count} messages. "
                    f"Latest: {symbol} = ${price_data['price']:.2f}"
                )

        except Exception as e:
            logger.error(f"Failed to store price in Redis for {symbol}: {e}")
            self.error_count += 1

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
                        continue

                    # Store in Redis
                    self._store_price_in_redis(symbol, price_data)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.error_count += 1

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

    try:
        # Create and start consumer (uses config defaults)
        consumer = PricesConsumer()
        consumer.start()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()