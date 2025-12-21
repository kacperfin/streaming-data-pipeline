#!/usr/bin/env python3
"""
Retrieves all Binance spot symbols.

This script queries the Binance API /api/v3/exchangeInfo endpoint
and returns all SPOT trading pairs in lowercase, comma-separated format.
"""

import requests

def get_spot_symbols() -> str:
    """
    Retrieve all Binance SPOT symbols.

    Returns:
        Comma-separated string of symbols in lowercase
    """
    endpoint = "https://api.binance.com/api/v3/exchangeInfo"

    try:
        response = requests.get(endpoint, timeout=10)
        response.raise_for_status()

        data = response.json()
        symbols = data.get('symbols', [])

        # Filter for SPOT symbols
        spot_symbols = []
        for symbol in symbols:
            if symbol.get('isSpotTradingAllowed', False):
                symbol_name = symbol.get('symbol', '')
                spot_symbols.append(symbol_name.lower())

        return ','.join(spot_symbols)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching symbols: {e}")
        return ""

def main():
    """Main execution function."""
    symbols = get_spot_symbols()

    if symbols:
        print(symbols)
    else:
        print("Failed to retrieve symbols.")

if __name__ == "__main__":
    main()