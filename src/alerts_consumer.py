"""
Alerts Consumer: Reads alerts from Kafka and stores them in Redis.

Consumes alert messages from the 'alerts' Kafka topic (produced by Spark processor)
and stores the last 200 alerts in a Redis list for display in Streamlit dashboard.
"""
import json
import logging
import sys
from time import time

import redis
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, start_http_server

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_ALERTS, KAFKA_CONSUMER_GROUP_ALERTS,
    REDIS_HOST, REDIS_PORT, REDIS_MAX_ALERTS, METRICS_PORT_ALERTS_CONSUMER
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('alerts_consumer')

# Prometheus metrics
CONSUMER_PROCESSING_LATENCY = Histogram(
    'consumer_processing_latency_seconds',
    'Time to process message (deserialize + store in Redis)',
    buckets=(0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0),
    labelnames=['consumer', 'symbol']
)

CONSUMER_ALERTS_STORED = Counter(
    'consumer_alerts_stored_total',
    'Total number of alerts stored in Redis',
    labelnames=['consumer', 'alert_type', 'symbol']
)

CONSUMER_ERRORS = Counter(
    'consumer_errors_total',
    'Total number of errors encountered by consumer',
    labelnames=['consumer', 'error_type']
)

class AlertsConsumer:
    """Consumer that reads alerts from Kafka and stores them in Redis."""

    def __init__(
        self,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic=KAFKA_TOPIC_ALERTS,
        kafka_group_id=KAFKA_CONSUMER_GROUP_ALERTS,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        max_alerts=REDIS_MAX_ALERTS
    ):
        """
        Initialize the Alerts Consumer.

        Args:
            kafka_bootstrap_servers: Kafka broker address
            kafka_topic: Kafka topic to consume from
            kafka_group_id: Consumer group ID
            redis_host: Redis host address
            redis_port: Redis port
            max_alerts: Maximum number of alerts to keep in Redis (default: 200)
        """
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_group_id = kafka_group_id
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.max_alerts = max_alerts
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
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                # Low-latency tuning
                fetch_min_bytes=1,
                fetch_max_wait_ms=10,
                # Performance tuning
                max_poll_records=100,
                max_poll_interval_ms=300000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
            )
            logger.info(f"Kafka consumer initialized. Topic: {self.kafka_topic}, Group: {self.kafka_group_id}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    def _store_alert_in_redis(self, symbol, alert_data):
        """
        Store alert in Redis list (FIFO, keeping last max_alerts entries).

        Args:
            symbol: Trading pair symbol (e.g., 'BTCUSDT')
            alert_data: Alert data dictionary from Spark
        """
        processing_start = time()

        try:
            # Store alerts in a Redis list: alerts
            # Format: JSON string with all alert details
            redis_key = "alerts"

            # Push to the right (newest alerts at the end)
            self.redis_client.rpush(redis_key, json.dumps(alert_data))  # type: ignore

            # Trim list to keep only last max_alerts entries
            # Keep the most recent alerts (right side)
            self.redis_client.ltrim(redis_key, -self.max_alerts, -1)  # type: ignore

            # Record metrics
            alert_type = alert_data.get('alert_type', 'unknown')
            CONSUMER_ALERTS_STORED.labels(
                consumer='alerts',
                alert_type=alert_type,
                symbol=symbol
            ).inc()

            processing_latency = time() - processing_start
            CONSUMER_PROCESSING_LATENCY.labels(consumer='alerts', symbol=symbol).observe(processing_latency)

            self.message_count += 1
            logger.info(
                f"Alert stored [{self.message_count}]: {symbol} {alert_type} "
                f"({alert_data.get('percent_change', 0)*100:.2f}% change)"
            )

        except Exception as e:
            logger.error(f"Failed to store alert in Redis for {symbol}: {e}")
            self.error_count += 1
            CONSUMER_ERRORS.labels(consumer='alerts', error_type='redis_store').inc()

    def start(self):
        """Start consuming messages from Kafka and storing them in Redis."""
        try:
            logger.info(f"Starting to consume from topic '{self.kafka_topic}'...")
            logger.info(f"Storing last {self.max_alerts} alerts in Redis list 'alerts'")
            logger.info("Waiting for messages...")

            # Consume messages indefinitely
            for message in self.consumer:  # type: ignore
                try:
                    symbol = message.key
                    alert_data = message.value

                    # Validate message structure
                    if not symbol or not isinstance(alert_data, dict):
                        logger.warning(f"Invalid message format: key={symbol}, value={alert_data}")
                        self.error_count += 1
                        CONSUMER_ERRORS.labels(consumer='alerts', error_type='invalid_message').inc()
                        continue

                    # Store in Redis
                    self._store_alert_in_redis(symbol, alert_data)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.error_count += 1
                    CONSUMER_ERRORS.labels(consumer='alerts', error_type='processing').inc()

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
            f"Consumer stopped. Total alerts processed: {self.message_count}, "
            f"Errors: {self.error_count}"
        )

def main():
    """Main entry point."""
    logger.info("Starting Alerts Consumer (Kafka -> Redis)")
    logger.info(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Source Kafka topic: {KAFKA_TOPIC_ALERTS}")
    logger.info(f"Consumer group: {KAFKA_CONSUMER_GROUP_ALERTS}")
    logger.info(f"Target Redis key: alerts")

    # Start Prometheus metrics HTTP server
    start_http_server(METRICS_PORT_ALERTS_CONSUMER)
    logger.info(f"Prometheus metrics available at http://localhost:{METRICS_PORT_ALERTS_CONSUMER}/metrics")

    try:
        # Create and start consumer (uses config defaults)
        consumer = AlertsConsumer()
        consumer.start()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()