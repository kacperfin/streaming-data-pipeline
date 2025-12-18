from dotenv import load_dotenv

import os

load_dotenv()

# Binance WebSocket

BINANCE_WS_BASE_URL = os.getenv('BINANCE_WS_BASE_URL', 'wss://stream.binance.com:9443/stream?streams=')
BINANCE_TRADING_PAIRS = os.getenv('BINANCE_TRADING_PAIRS', 'btcusdt,ethusdt')
BINANCE_MODE = os.getenv('BINANCE_MODE', 'aggTrade')
BINANCE_LIST_OF_STREAMS = [f'{pair.lower()}@{BINANCE_MODE}' for pair in BINANCE_TRADING_PAIRS.split(',') if len(pair) > 1]
BINANCE_SOCKET_URL = BINANCE_WS_BASE_URL + '/'.join(BINANCE_LIST_OF_STREAMS) + '&timeUnit=MICROSECOND'

# Kafka

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC_PRICES = os.getenv('KAFKA_TOPIC_PRICES', 'prices')
KAFKA_TOPIC_ALERTS = os.getenv('KAFKA_TOPIC_ALERTS', 'alerts')
KAFKA_CONSUMER_GROUP_PRICES = os.getenv('KAFKA_CONSUMER_GROUP_PRICES', 'prices_consumer_group')
KAFKA_CONSUMER_GROUP_ALERTS = os.getenv('KAFKA_CONSUMER_GROUP_ALERTS', 'alerts_consumer_group')
KAFKA_PRICES_TOPIC_NUM_PARTITIONS = int(os.getenv('KAFKA_PRICES_TOPIC_NUM_PARTITIONS', '3'))
KAFKA_ALERTS_TOPIC_NUM_PARTITIONS = int(os.getenv('KAFKA_ALERTS_TOPIC_NUM_PARTITIONS', '3'))

# Redis

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_MAX_ALERTS = int(os.getenv('REDIS_MAX_ALERTS', '200'))

# Spark

SPARK_ALERT_THRESHOLD = float(os.getenv('SPARK_ALERT_THRESHOLD', '0.0025'))

# Streamlit

STREAMLIT_REFRESH_RATE = float(os.getenv('STREAMLIT_REFRESH_RATE', 0.2))

# Clock Skew Monitor

CLOCK_SKEW_MONITOR_INTERVAL_SECONDS = int(os.getenv('CLOCK_SKEW_MONITOR_INTERVAL_SECONDS', 5))