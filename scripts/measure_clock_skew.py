#!/usr/bin/env python3
"""
Measure clock skew between local system and Binance server.

This script measures the time difference between the local system clock
and Binance's server time to help diagnose timing-related issues in the
streaming pipeline.
"""

import time
import requests
import numpy as np

def measure_single_skew() -> tuple[float, float]:
    """
    Measure clock skew for a single request.

    Returns:
        Tuple of (skew_ms, round_trip_ms)
        - skew_ms: Local time - Binance server time (positive means local is ahead)
        - round_trip_ms: Total round-trip time for the request
    """
    # Capture time before request
    local_before = time.time() * 1000  # Convert to milliseconds

    # Make request to Binance
    response = requests.get('https://api.binance.com/api/v3/time', timeout=5)
    response.raise_for_status()

    # Capture time after request
    local_after = time.time() * 1000

    # Get Binance server time
    binance_time = response.json()['serverTime']

    # Estimate local time at the moment Binance generated the timestamp
    # Assume the request took half the round-trip time
    round_trip = local_after - local_before
    estimated_local_time = local_before + (round_trip / 2)

    # Calculate skew (positive means local clock is ahead)
    skew = estimated_local_time - binance_time

    return skew, round_trip


def measure_clock_skew(num_samples: int = 10) -> None:
    """
    Measure clock skew multiple times and report statistics.

    Args:
        num_samples: Number of measurements to take
    """
    print(f"Measuring clock skew against Binance server...")
    print(f"Taking {num_samples} samples...\n")

    skews: list[float] = []
    round_trips: list[float] = []

    for i in range(num_samples):
        try:
            skew, round_trip = measure_single_skew()
            skews.append(skew)
            round_trips.append(round_trip)

            print(f"Sample {i+1}: skew={skew:+.2f}ms, round_trip={round_trip:.2f}ms")

            # Pause between requests to be nice to the API
            if i < num_samples - 1:
                time.sleep(3)

        except Exception as e:
            print(f"Sample {i+1}: ERROR - {e}")

    if not skews:
        print("\nERROR: No successful measurements")
        return

    # Calculate statistics using numpy
    skews_array = np.array(skews)
    round_trips_array = np.array(round_trips)

    print("\n" + "="*60)
    print("CLOCK SKEW STATISTICS")
    print("="*60)
    print(f"Mean skew:           {np.mean(skews_array):+.2f} ms")
    print(f"Median skew:         {np.median(skews_array):+.2f} ms")
    print(f"Std deviation:       {np.std(skews_array):.2f} ms")
    print(f"Min skew:            {np.min(skews_array):+.2f} ms")
    print(f"Max skew:            {np.max(skews_array):+.2f} ms")
    print()
    print(f"Mean round-trip:     {np.mean(round_trips_array):.2f} ms")
    print(f"Median round-trip:   {np.median(round_trips_array):.2f} ms")
    print()

    mean_skew = np.mean(skews_array)
    if mean_skew > 0:
        print(f"Your local clock is {abs(mean_skew):.2f}ms AHEAD of Binance.")
        print("This means measured latencies will appear HIGHER than actual.")
    else:
        print(f"Your local clock is {abs(mean_skew):.2f}ms BEHIND Binance.")
        print("This means measured latencies will appear LOWER than actual.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Measure clock skew between local system and Binance server"
    )
    parser.add_argument(
        "-n", "--num-samples",
        type=int,
        default=10,
        help="Number of samples to take (default: 10)"
    )

    args = parser.parse_args()

    try:
        measure_clock_skew(args.num_samples)
    except KeyboardInterrupt:
        print("\n\nMeasurement interrupted by user")
    except Exception as e:
        print(f"\nERROR: {e}")
        exit(1)