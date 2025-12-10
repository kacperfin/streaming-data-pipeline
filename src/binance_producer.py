import json
import logging
import sys
from time import time

import websocket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError
from prometheus_client import Counter, Histogram, start_http_server

from config import BINANCE_SOCKET_URL, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PRICES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('binance_producer')

# Prometheus metrics
PRODUCER_LATENCY = Histogram(
    'producer_latency_seconds',
    'Time from receiving message from Binance to sending to Kafka',
    # Buckets optimized for sub-millisecond latencies (50µs to 100ms range)
    buckets=(0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1),
    labelnames=['symbol']  # Add label for symbol
)

MESSAGES_RECEIVED_TOTAL = Counter(
    'producer_messages_received_total',
    'Total number of messages received from Binance WebSocket',
    labelnames=['symbol']
)

MESSAGES_SENT_TOTAL = Counter(
    'producer_messages_sent_total',
    'Total number of messages successfully sent to Kafka',
    labelnames=['symbol']
)

ERRORS_TOTAL = Counter(
    'producer_errors_total',
    'Total number of errors encountered',
    labelnames=['error_type']  # e.g., 'json_decode', 'kafka_send', 'processing'
)

BINANCE_NETWORK_LATENCY = Histogram(
    'binance_network_latency_seconds',
    'Network latency from Binance timestamp to producer receive time',
    # Buckets optimized for network latencies (10ms to 5s range)
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    labelnames=['symbol']
)

class BinanceKafkaProducer:
    """Producer that streams data from Binance WebSocket to Kafka."""

    def __init__(self, kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, kafka_topic=KAFKA_TOPIC_PRICES):
        """
        Initialize the producer.

        Args:
            kafka_bootstrap_servers: Kafka broker address
            kafka_topic: Kafka topic to send messages to
        """
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self.ws = None
        self.received_count = 0  # Messages received from Binance WebSocket
        self.message_count = 0   # Messages acknowledged by Kafka
        self.error_count = 0

        # Ensure Kafka topic exists with proper configuration
        self._create_topic_if_not_exists()

        # Initialize Kafka producer
        self._init_kafka_producer()

    def _create_topic_if_not_exists(self):
        """Create Kafka topic with proper configuration if it doesn't exist."""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_bootstrap_servers,
                client_id='binance_producer_admin'
            )

            # Define topic with 3 partitions and replication factor 1 (single-node setup)
            topic = NewTopic(
                name=self.kafka_topic,
                num_partitions=3,
                replication_factor=1,
                topic_configs={
                    'retention.ms': '900000',  # 15 minutes = 900000 ms
                    'retention.bytes': '2147483648'  # 2GB per partition
                }
            )

            # Try to create the topic
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            logger.info(f"Topic '{self.kafka_topic}' created successfully with 3 partitions")

        except TopicAlreadyExistsError:
            logger.info(f"Topic '{self.kafka_topic}' already exists")
        except Exception as e:
            logger.warning(f"Error checking/creating topic: {e}. Will attempt to proceed anyway.")
        finally:
            try:
                admin_client.close()
            except:
                pass

    def _init_kafka_producer(self):
        """Initialize Kafka producer with proper configuration."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                # Delivery guarantees
                acks=1, # Leader acknowledgment (sufficient for single-node setup)
                retries=3,
                # Performance tuning - allows multiple requests in flight for better throughput
                max_in_flight_requests_per_connection=10,
                # Batching for better throughput
                linger_ms=10,
                batch_size=16384,
            )
            logger.info(f"Kafka producer initialized. Bootstrap servers: {self.kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def on_message(self, ws, message):
        """
        Handle incoming WebSocket messages.

        Args:
            ws: WebSocket instance
            message: Raw message from Binance
        """
        # Start timing - capture when we received the message
        start_time = time()

        try:
            # Parse the message
            data = json.loads(message)

            # Binance sends data in a specific format with 'stream' and 'data' fields
            if 'data' in data:
                trade_data = data['data']
                symbol = trade_data.get('s', 'unknown').lower()

                price = float(trade_data.get('p')) # Trade price
                binance_timestamp = int(trade_data.get('T'))

                # Calculate network latency: Binance event time → Producer receive time
                receive_time_us = start_time * 1_000_000  # Convert to microseconds
                network_latency_seconds = (receive_time_us - binance_timestamp) / 1_000_000
                BINANCE_NETWORK_LATENCY.labels(symbol=symbol).observe(network_latency_seconds)

                # Create structured message for Kafka
                kafka_message = {
                    'price': price,
                    'binance_timestamp': binance_timestamp
                }

                # Send to Kafka with symbol as key for partitioning
                self._send_to_kafka(symbol, kafka_message)

                # Record metrics
                self.received_count += 1
                MESSAGES_RECEIVED_TOTAL.labels(symbol=symbol).inc()
                latency = time() - start_time
                PRODUCER_LATENCY.labels(symbol=symbol).observe(latency)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
            self.error_count += 1
            ERRORS_TOTAL.labels(error_type='json_decode').inc()
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.error_count += 1
            ERRORS_TOTAL.labels(error_type='processing').inc()

    def _send_to_kafka(self, key, message):
        """
        Send message to Kafka.

        Args:
            key: Message key (symbol) for partitioning
            message: Message data
        """
        try:
            future = self.producer.send(self.kafka_topic, key=key, value=message) # type: ignore
            # Add callbacks for delivery confirmation
            # Pass symbol to both callbacks so we can track metrics with the right label
            future.add_callback(lambda metadata: self._on_send_success(metadata, key))
            future.add_errback(lambda exception: self._on_send_error(exception, key))

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            self.error_count += 1
            ERRORS_TOTAL.labels(error_type='kafka_error').inc()
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            self.error_count += 1
            ERRORS_TOTAL.labels(error_type='kafka_send').inc()

    def _on_send_success(self, record_metadata, symbol):
        """Callback for successful message delivery."""
        self.message_count += 1
        # Increment counter only when Kafka acknowledges (accurate tracking)
        MESSAGES_SENT_TOTAL.labels(symbol=symbol).inc()

        if self.message_count % 100 == 0:
            logger.info(
                f"Kafka acknowledged {self.message_count} messages total. "
                f"Latest: partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )

    def _on_send_error(self, exception, symbol):
        """Callback for failed message delivery."""
        logger.error(f"Failed to send message for {symbol}: {exception}")
        self.error_count += 1
        # Track which symbol had the error
        ERRORS_TOTAL.labels(error_type='kafka_send_async').inc()

    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close."""
        logger.info(f"WebSocket connection closed. Status: {close_status_code}, Message: {close_msg}")

    def on_open(self, ws):
        """Handle WebSocket connection open."""
        logger.info(f"WebSocket connection opened to: {BINANCE_SOCKET_URL}")
        logger.info("Starting to receive cryptocurrency price data...")

    def start(self):
        """Start the producer - connect to Binance and stream to Kafka."""
        try:
            logger.info(f"Connecting to Binance WebSocket: {BINANCE_SOCKET_URL}")

            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                BINANCE_SOCKET_URL,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )

            # Run the WebSocket connection (blocks until connection closes)
            self.ws.run_forever()

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Error in producer: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        """Stop the producer and cleanup resources."""
        logger.info("Stopping producer...")

        # Close WebSocket connection
        if self.ws:
            self.ws.close()

        # Flush and close Kafka producer
        if self.producer:
            logger.info("Flushing remaining messages to Kafka...")
            # Flush ensures all buffered messages are sent before shutdown (blocks until complete)
            self.producer.flush()
            self.producer.close()

        # Log statistics
        logger.info(f"Producer stopped. Total messages sent: {self.message_count}, Errors: {self.error_count}")

def main():
    """Main entry point."""
    logger.info("Starting Binance -> Kafka Producer")
    logger.info(f"Target Kafka topic: {KAFKA_TOPIC_PRICES}")
    logger.info(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    # Start Prometheus metrics HTTP server
    metrics_port = 8000
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics available at http://localhost:{metrics_port}/metrics")

    try:
        # Create and start producer (uses config defaults)
        producer = BinanceKafkaProducer()
        producer.start()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()