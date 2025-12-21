"""
Binance WebSocket connection test script.

Connects to Binance WebSocket API and displays incoming messages for 15 seconds.
Reports statistics including total messages received and messages per second.
Useful for verifying connectivity and estimating throughput before running experiments.
"""

import json
import sys
import threading
from datetime import datetime
from pathlib import Path

import websocket

sys.path.append(str(Path(__file__).parent.parent / "src"))

from config import BINANCE_SOCKET_URL, BINANCE_LIST_OF_STREAMS

DURATION_SECONDS = 15

def on_message(ws, message):
    global message_count
    message_count += 1

    data = json.loads(message)

    print(f"[{message_count}] {datetime.now().strftime('%H:%M:%S.%f')} - {data}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print(f"\n--- Connection closed ---")
    print(f"Total messages received: {message_count}")
    print(f"Messages per second: {message_count / DURATION_SECONDS:.2f}")
    print(f"List of streams: {', '.join(BINANCE_LIST_OF_STREAMS)}")
    print(f"Number of streams: {len(BINANCE_LIST_OF_STREAMS)}")

def on_open(ws):
    print(f"--- Connection opened at {datetime.now().strftime('%H:%M:%S')} ---")
    print(f"Listening for {DURATION_SECONDS} seconds...\n")

if __name__ == "__main__":
    message_count = 0

    ws = websocket.WebSocketApp(
        BINANCE_SOCKET_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    def stop_websocket():
        ws.close()

    timer = threading.Timer(DURATION_SECONDS, stop_websocket)
    timer.start()

    ws.run_forever()