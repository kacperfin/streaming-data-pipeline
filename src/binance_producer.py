import json
import logging
import sys
import time

import websocket
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from config import BINANCE_SOCKET_URL, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PRICES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('binance_producer')

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
                replication_factor=1
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
        try:
            # Parse the message
            data = json.loads(message)

            # Binance sends data in a specific format with 'stream' and 'data' fields
            if 'data' in data:
                trade_data = data['data']
                symbol = trade_data.get('s', 'Unknown').lower()

                # Create structured message for Kafka
                # Minimal schema optimized for price alerts and latency measurement
                kafka_message = {
                    'price': float(trade_data.get('p', 0)),  # Trade price
                    'binance_timestamp': trade_data.get('T', int(time.time() * 1_000_000)),  # Binance trade timestamp (microseconds)
                    'producer_timestamp': int(time.time() * 1_000_000),  # Producer timestamp (microseconds)
                }

                # Send to Kafka with symbol as key for partitioning
                self._send_to_kafka(symbol, kafka_message)
                self.received_count += 1

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
            self.error_count += 1
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.error_count += 1

    def _send_to_kafka(self, key, message):
        """
        Send message to Kafka.

        Args:
            key: Message key (symbol) for partitioning
            message: Message data
        """
        try:
            future = self.producer.send(self.kafka_topic, key=key, value=message) # type: ignore
            # Add callback for delivery confirmation
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            self.error_count += 1
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            self.error_count += 1

    def _on_send_success(self, record_metadata):
        """Callback for successful message delivery."""
        self.message_count += 1
        if self.message_count % 100 == 0:
            logger.info(
                f"Kafka acknowledged {self.message_count} messages total. "
                f"Received from Binance: {self.received_count}. "
                f"In-flight: {self.received_count - self.message_count}. "
                f"Latest: topic={record_metadata.topic}, "
                f"partition={record_metadata.partition}, "
                f"offset={record_metadata.offset}"
            )

    def _on_send_error(self, exception):
        """Callback for failed message delivery."""
        logger.error(f"Failed to send message: {exception}")
        self.error_count += 1

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

    try:
        # Create and start producer (uses config defaults)
        producer = BinanceKafkaProducer()
        producer.start()

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()