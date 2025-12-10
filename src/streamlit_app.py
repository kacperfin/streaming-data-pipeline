import json
from time import sleep
from datetime import datetime
import streamlit as st
import redis

from config import REDIS_HOST, REDIS_PORT, BINANCE_TRADING_PAIRS, STREAMLIT_REFRESH_RATE

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
                if len(prices) == 1:
                    st.subheader(f"Current prices ({len(prices)} symbol)")
                else:
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
                        # Binance timestamp is in microseconds, convert to seconds for datetime
                        timestamp_seconds = float(price_data["binance_timestamp"]) / 1_000_000
                        st.caption(f'Last trade time (UTC): {datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')

            # Footer with refresh info
            st.divider()
            st.caption(f"Connected to Redis: {REDIS_HOST}:{REDIS_PORT}")

        # Refresh every 1 second
        sleep(STREAMLIT_REFRESH_RATE)

if __name__ == "__main__":
    main()
