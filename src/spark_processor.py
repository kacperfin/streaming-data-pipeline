"""
Spark Structured Streaming processor for price alerts.

Reads price data from Kafka, calculates rolling averages,
and generates buy/sell alerts when price deviates significantly.
"""

import logging
import threading

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from pyspark.sql import SparkSession
from pyspark.sql.streaming.listener import StreamingQueryListener
from pyspark.sql.functions import (
    col, from_json, window, min_by, max_by,
    when, abs as spark_abs, lit
)
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

from pyspark.sql.functions import to_json, struct, current_timestamp

from prometheus_client import Counter, Histogram, start_http_server

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PRICES, KAFKA_TOPIC_ALERTS,
    SPARK_ALERT_THRESHOLD, KAFKA_ALERTS_TOPIC_NUM_PARTITIONS,
    METRICS_PORT_SPARK_PROCESSOR
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('spark_processor')

# Prometheus metrics
SPARK_BATCH_DURATION = Histogram(
    'spark_batch_duration_seconds',
    'Time taken to process each micro-batch',
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0)
)

SPARK_INPUT_ROWS = Counter(
    'spark_input_rows_total',
    'Total number of input rows processed'
)

SPARK_OUTPUT_ROWS = Counter(
    'spark_output_rows_total',
    'Total number of output rows (alerts) generated'
)

SPARK_BATCHES_COMPLETED = Counter(
    'spark_batches_completed_total',
    'Total number of batches completed'
)

SPARK_ERRORS = Counter(
    'spark_errors_total',
    'Total number of processing errors',
    labelnames=['error_type']
)

# StreamingQueryListener to capture batch metrics
class MetricsListener(StreamingQueryListener):
    """Listener for Spark Structured Streaming query events."""

    def onQueryProgress(self, event):
        """Called when a batch completes successfully."""
        try:
            progress = event.progress

            # Batch processing metrics
            batch_duration = progress.batchDuration / 1000.0  # Convert ms to seconds
            SPARK_BATCH_DURATION.observe(batch_duration)

            # Input/output rows
            num_input_rows = progress.numInputRows
            SPARK_INPUT_ROWS.inc(num_input_rows)

            # Output rows (alerts generated)
            if progress.sink and hasattr(progress.sink, 'numOutputRows'):
                num_output_rows = progress.sink.numOutputRows
                SPARK_OUTPUT_ROWS.inc(num_output_rows)

            # Batch completion counter
            SPARK_BATCHES_COMPLETED.inc()

            logger.info(
                f"Batch {progress.batchId}: "
                f"duration={batch_duration:.3f}s, "
                f"input_rows={num_input_rows}, "
                f"rate={num_input_rows/batch_duration:.1f} rows/s"
            )

        except Exception as e:
            logger.error(f"Error in MetricsListener.onQueryProgress: {e}")
            SPARK_ERRORS.labels(error_type='listener_error').inc()

    def onQueryStarted(self, event):
        """Called when query starts."""
        pass

    def onQueryTerminated(self, event):
        """Called when query terminates."""
        if event.exception:
            logger.error(f"Query terminated with exception: {event.exception}")
            SPARK_ERRORS.labels(error_type='query_terminated').inc()


# Schema for price messages from Kafka
# Matches the format produced by binance_producer.py
PRICE_SCHEMA = StructType([
    StructField("price", DoubleType(), True),
    StructField("binance_timestamp", LongType(), True),
    StructField("received_timestamp", LongType(), True)
])

def _create_topic_if_not_exists():
    """Create alerts topic if it doesn't exist."""
    admin_client = None
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='spark_processor_admin'
        )

        topic = NewTopic(
            name=KAFKA_TOPIC_ALERTS,
            num_partitions=KAFKA_ALERTS_TOPIC_NUM_PARTITIONS,
            replication_factor=1,
            topic_configs={
                'retention.ms': '900000',  # 15 minutes
                'retention.bytes': '2147483648'  # 2GB per partition
            }
        )

        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info(f"Created topic '{KAFKA_TOPIC_ALERTS}' with {KAFKA_ALERTS_TOPIC_NUM_PARTITIONS} partitions")

    except TopicAlreadyExistsError:
        logger.info(f"Topic '{KAFKA_TOPIC_ALERTS}' already exists")
    except Exception as e:
        logger.warning(f"Could not create topic: {e}")
    finally:
        if admin_client:
            try:
                admin_client.close()
            except:
                pass

def create_spark_session():
    """Create and configure Spark session for streaming."""
    spark = SparkSession.builder \
        .appName("PriceAlertProcessor") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", str(KAFKA_ALERTS_TOPIC_NUM_PARTITIONS)) \
        .getOrCreate()

    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    # Attach metrics listener
    listener = MetricsListener()
    spark.streams.addListener(listener)
    logger.info("Attached MetricsListener to Spark streaming queries")

    return spark

def main():
    """Main entry point for Spark processor."""
    logger.info("Starting Spark Processor")
    logger.info(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Source topic: {KAFKA_TOPIC_PRICES}")

    # Start Prometheus metrics server in background thread
    metrics_thread = threading.Thread(
        target=start_http_server,
        args=(METRICS_PORT_SPARK_PROCESSOR,),
        daemon=True,
        name="prometheus-metrics"
    )
    metrics_thread.start()
    logger.info(f"Prometheus metrics server started on port {METRICS_PORT_SPARK_PROCESSOR}")

    # Create alerts topic if needed
    _create_topic_if_not_exists()

    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session created")

    # Read from Kafka
    # The key is the symbol (e.g., "btcusdt"), value is the JSON price data
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_PRICES) \
        .option("kafka.group.id", "spark_processor_group") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    logger.info("Kafka stream configured")

    # Parse the Kafka message
    # - key: symbol (string)
    # - value: JSON with price data
    # Convert binance_timestamp (microseconds) to proper timestamp for windowing
    parsed_df = kafka_df.select(
        col("key").cast("string").alias("symbol"),
        from_json(col("value").cast("string"), PRICE_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("symbol"),
        col("data.price").alias("price"),
        col("data.binance_timestamp").alias("binance_timestamp"),
        (col("data.binance_timestamp") / 1000000).cast("timestamp").alias("binance_ts"),
        col("data.received_timestamp").alias("received_timestamp"),
        col("kafka_timestamp")
    )

    # Watermark on Binance time: allow 1 second for late data
    watermarked_df = parsed_df.withWatermark("binance_ts", "1 second")

    # 1-minute tumbling window based on Binance timestamps
    # Windows align exactly with Binance candles (verifiable on TradingView)
    # - open_price: price with earliest binance_timestamp in window
    # - close_price: price with latest binance_timestamp in window
    # - percent_change: (close - open) / open
    windowed_df = watermarked_df.groupBy(
        col("symbol"),
        window(col("binance_ts"), "1 minute")
    ).agg(
        min_by("price", "binance_timestamp").alias("open_price"),
        max_by("price", "binance_timestamp").alias("close_price")
    ).withColumn(
        "percent_change", (col("close_price") - col("open_price")) / col("open_price")
    )

    # Filter for alerts: price change exceeds threshold
    # SELL signal: price increased > threshold (time to sell high)
    # BUY signal: price decreased > threshold (time to buy low)
    alerts_df = windowed_df.filter(
        spark_abs(col("percent_change")) > SPARK_ALERT_THRESHOLD
    ).withColumn(
        "alert_type",
        when(col("percent_change") > 0, lit("SELL")).otherwise(lit("BUY"))
    ).select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("open_price"),
        col("close_price"),
        col("percent_change"),
        col("alert_type"),
        current_timestamp().alias("alert_timestamp")
    )

    # Write alerts to Kafka
    # Key: symbol, Value: JSON with alert details
    kafka_query = alerts_df.select(
        col("symbol").alias("key"),
        to_json(struct(
            col("symbol"),
            col("window_start"),
            col("window_end"),
            col("open_price"),
            col("close_price"),
            col("percent_change"),
            col("alert_type"),
            col("alert_timestamp")
        )).alias("value")
    ).writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC_ALERTS) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/alerts") \
        .trigger(processingTime="0 seconds") \
        .start()

    logger.info("Streaming query started")
    logger.info(f"Alert threshold: {SPARK_ALERT_THRESHOLD * 100}%")
    logger.info(f"Alerts will be written to Kafka topic: {KAFKA_TOPIC_ALERTS}")
    logger.info("Press Ctrl+C to stop")

    # Wait for query termination
    kafka_query.awaitTermination()

if __name__ == "__main__":
    main()