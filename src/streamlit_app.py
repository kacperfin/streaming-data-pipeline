import json
import time
from datetime import datetime
import streamlit as st
import redis

from config import REDIS_HOST, REDIS_PORT, BINANCE_TRADING_PAIRS

# Page configuration
st.set_page_config(
    page_title="Cryptocurrencies",
    page_icon="📈"
)

# Initialize Redis connection
@st.cache_resource
def get_redis_client():
    """Get Redis client connection."""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True
    )

def fetch_prices(redis_client):
    """Fetch all current prices from Redis in the order specified in config."""
    prices = {}

    # Get symbols in the order specified in .env
    ordered_symbols = [pair.strip().lower() for pair in BINANCE_TRADING_PAIRS.split(',') if pair.strip()]

    for symbol in ordered_symbols:
        try:
            key = f'price:{symbol}'
            # Get price data
            price_json = redis_client.get(key)
            if price_json:
                price_data = json.loads(price_json)
                prices[symbol] = price_data
        except Exception as e:
            st.error(f"Error fetching {key}: {e}")

    return prices

def format_timestamp(ts_microseconds):
    """Convert microsecond timestamp to readable format."""
    try:
        ts_seconds = ts_microseconds / 1_000_000
        dt = datetime.fromtimestamp(ts_seconds)
        return dt.strftime('%H:%M:%S.%f')  # Show full microseconds
    except:
        return "N/A"

def calculate_latency_ms(start_ts, end_ts):
    """Calculate latency in milliseconds between two microsecond timestamps."""
    try:
        return (end_ts - start_ts) / 1_000  # Convert microseconds to milliseconds
    except:
        return None

def main():
    """Main Streamlit app."""
    st.title("Cryptocurrencies")

    # Get Redis client
    redis_client = get_redis_client()

    # Test Redis connection
    try:
        redis_client.ping()
    except Exception as e:
        st.error(f"Cannot connect to Redis: {e}")
        st.info(f"Trying to connect to: {REDIS_HOST}:{REDIS_PORT}")
        st.stop()

    # Create placeholder for dynamic content
    placeholder = st.empty()

    # Auto-refresh loop
    while True:
        with placeholder.container():
            # Fetch current prices
            prices = fetch_prices(redis_client)

            if not prices:
                st.warning("Waiting for price data...")
            else:
                # Display prices in columns
                st.subheader(f"Current prices ({len(prices)} symbols)")

                # Create columns for price display
                cols = st.columns(min(3, len(prices)))

                for idx, (symbol, price_data) in enumerate(prices.items()):
                    col_idx = idx % 3
                    with cols[col_idx]:
                        # Display price card
                        st.metric(
                            label=symbol.upper(),
                            value=f"${float(price_data['price']):,.2f}"
                        )

                        # Show additional info in expander
                        with st.expander("Details"):
                            # Get timestamps
                            binance_ts = price_data.get('binance_timestamp')
                            producer_ts = price_data.get('producer_timestamp')
                            consumer_ts = price_data.get('consumer_timestamp')

                            # Display formatted timestamps
                            st.write(f"**Binance Time:** {format_timestamp(binance_ts) if binance_ts else 'N/A'}")
                            st.write(f"**Producer Time:** {format_timestamp(producer_ts) if producer_ts else 'N/A'}")
                            st.write(f"**Consumer Time:** {format_timestamp(consumer_ts) if consumer_ts else 'N/A'}")

                            # Calculate and display latencies
                            if binance_ts and producer_ts and consumer_ts:
                                st.divider()
                                producer_latency = calculate_latency_ms(binance_ts, producer_ts)
                                kafka_consumer_latency = calculate_latency_ms(producer_ts, consumer_ts)
                                total_latency = calculate_latency_ms(binance_ts, consumer_ts)

                                if producer_latency is not None:
                                    st.write(f"**Producer Latency:** {producer_latency:.3f} ms")
                                if kafka_consumer_latency is not None:
                                    st.write(f"**Kafka + Consumer:** {kafka_consumer_latency:.3f} ms")
                                if total_latency is not None:
                                    st.write(f"**Total Latency:** {total_latency:.3f} ms")

            # Footer with refresh info
            st.divider()
            st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            st.caption(f"Connected to Redis: {REDIS_HOST}:{REDIS_PORT}")

        # Refresh every 1 second
        time.sleep(1)

if __name__ == "__main__":
    main()
