import json
from time import sleep
from datetime import datetime
import streamlit as st
import redis

from config import REDIS_HOST, REDIS_PORT, BINANCE_TRADING_PAIRS, STREAMLIT_REFRESH_RATE

# Page configuration
st.set_page_config(
    page_title="Cryptocurrencies",
    page_icon="📈",
    layout="wide"
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

def fetch_alerts(redis_client, max_alerts=20):
    """Fetch recent alerts from Redis list (last max_alerts entries)."""
    try:
        # Get last max_alerts from the 'alerts' list
        # -max_alerts to -1 gets the last max_alerts entries
        alerts_json = redis_client.lrange('alerts', -max_alerts, -1)

        # Parse JSON and reverse to show newest first
        alerts = [json.loads(alert_json) for alert_json in alerts_json]
        alerts.reverse()

        return alerts
    except Exception as e:
        st.error(f"Error fetching alerts: {e}")
        return []

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
                        price = float(price_data['price'])
                        st.metric(
                            label=symbol.upper(),
                            value=f"${price:,}"
                        )
                        # Binance timestamp is in microseconds, convert to seconds for datetime
                        timestamp_seconds = float(price_data["binance_timestamp"]) / 1_000_000
                        st.caption(f'Last trade time (UTC): {datetime.fromtimestamp(timestamp_seconds).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}')

            # Alerts section
            st.divider()
            st.subheader("Recent Alerts")

            # Display current time
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
            st.caption(f"Current time: {current_time}")

            alerts = fetch_alerts(redis_client, max_alerts=20)

            if not alerts:
                st.info("No alerts yet. Alerts are generated when price changes by more than the threshold within a 1-minute window.")
            else:
                if len(alerts) == 1:
                    st.caption(f"Showing last {len(alerts)} alert")
                else:
                    st.caption(f"Showing last {len(alerts)} alerts")

                # Display alerts as expandable cards
                for alert in alerts:
                    symbol = alert.get('symbol', 'UNKNOWN')
                    alert_type = alert.get('alert_type', 'UNKNOWN')
                    percent_change = alert.get('percent_change', 0) * 100
                    open_price = alert.get('open_price', 0)
                    close_price = alert.get('close_price', 0)
                    window_start = alert.get('window_start')
                    window_end = alert.get('window_end')
                    alert_timestamp = alert.get('alert_timestamp')

                    # Color based on alert type
                    if alert_type == 'BUY':
                        emoji = "🟢"
                        color = "green"
                    elif alert_type == 'SELL':
                        emoji = "🔴"
                        color = "red"
                    else:
                        emoji = "⚪"
                        color = "gray"

                    # Format: "🟢 BUY BTCUSDT: -2.5% ($95000 -> $92625)"
                    # Build price display to avoid markdown rendering issues
                    price_display = f"${open_price:,} → ${close_price:,}".replace("$", "\\$")
                    with st.expander(
                        f"{emoji} **{alert_type}** {symbol.upper()}: "
                        f"{percent_change:+.2f}% ({price_display})",
                        expanded=False
                    ):
                        st.markdown(f"**Alert Type:** :{color}[{alert_type}]")
                        st.markdown(f"**Symbol:** {symbol.upper()}")
                        st.markdown(f"**Price Change:** {percent_change:+.2f}%")
                        st.markdown(f"**Open Price:** ${open_price:,}")
                        st.markdown(f"**Close Price:** ${close_price:,}")
                        if window_start:
                            st.markdown(f"**Window Start:** {window_start}")
                        if window_end:
                            st.markdown(f"**Window End:** {window_end}")
                        if alert_timestamp:
                            st.markdown(f"**Alert Generated:** {alert_timestamp}")

            # Footer with refresh info
            st.divider()
            st.caption(f"Connected to Redis: {REDIS_HOST}:{REDIS_PORT}")

        # Refresh every 1 second
        sleep(STREAMLIT_REFRESH_RATE)

if __name__ == "__main__":
    main()
