import time
import requests
import logging
import sys
import os
from prometheus_client import start_http_server, Gauge, Counter, Histogram

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('clock_skew_monitor')

from config import CLOCK_SKEW_MONITOR_INTERVAL_SECONDS

# Prometheus Metrics
CLOCK_SKEW = Gauge(
    'binance_clock_skew_seconds',
    'Difference: Local Time - Binance Time in seconds (Positive = Local is Ahead)'
)

NETWORK_ROUND_TRIP = Gauge(
    'binance_api_round_trip_seconds',
    'Round-trip time for Binance REST API time check request'
)

REQUEST_ERRORS = Counter(
    'binance_time_request_errors_total',
    'Total number of failed time check requests'
)

def measure_skew():
    """
    Measure clock skew against Binance.
    """
    try:
        # Capture time before request (ms)
        local_before = time.time() * 1000

        # Make request to Binance (Weight: 1)
        response = requests.get('https://api.binance.com/api/v3/time', timeout=5)
        response.raise_for_status()

        # Capture time after request (ms)
        local_after = time.time() * 1000

        # Get Binance server time
        binance_time = response.json()['serverTime']

        # Estimate local time at the moment Binance generated the timestamp
        # Assume the request took half the round-trip time
        round_trip = local_after - local_before
        estimated_local_time = local_before + (round_trip / 2)

        # Calculate skew (positive means local clock is ahead)
        skew_seconds = (estimated_local_time - binance_time) / 1000
        round_trip_seconds = round_trip / 1000

        # Update Metrics
        CLOCK_SKEW.set(skew_seconds)
        NETWORK_ROUND_TRIP.set(round_trip_seconds)

        logger.info(f"Skew: {skew_seconds*1000:+.2f}ms | Round-trip: {round_trip:.2f}ms")

    except Exception as e:
        logger.error(f"Error checking Binance time: {e}")
        REQUEST_ERRORS.inc()

def main():
    logger.info("Starting Clock Skew Monitor")
    logger.info(f"Check interval: {CLOCK_SKEW_MONITOR_INTERVAL_SECONDS} seconds")

    # Start Prometheus metrics HTTP server
    metrics_port = 8004
    start_http_server(metrics_port)
    logger.info(f"Prometheus metrics available at http://localhost:{metrics_port}/metrics")

    # Main Loop
    while True:
        measure_skew()
        time.sleep(CLOCK_SKEW_MONITOR_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)