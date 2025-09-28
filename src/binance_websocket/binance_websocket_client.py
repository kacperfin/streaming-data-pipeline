import websocket

import json
import os

from .utils.print_formatted_message import print_formatted_message

class BinanceWebsocketClient():
    def __init__(self, symbols: list[str],  mode: str):
        BASE_URL = f'wss://stream.binance.com:9443/stream?streams='
        socket_url = _create_socket_url(BASE_URL, symbols, mode)

        self.ws = websocket.WebSocketApp(socket_url, on_open=self._on_open,
                                                     on_message=self._on_message,
                                                     on_error=self._on_error,
                                                     on_close=self._on_close)

    def _on_message(self, ws, message):
        message = json.loads(message)

        os.system('clear')
        print_formatted_message(message)

    def _on_error(self, ws, error):
        print(f'error: {error}')

    def _on_close(self, ws, close_status_code, close_msg):
        print('### Connection closed ###')
        print(f'close status code: {close_status_code}')
        print(f'close message: {close_msg}')

    def _on_open(self, ws):
        print('### Connection opened ###')

    def run_forever(self):
        self.ws.run_forever()

def _create_socket_url(base_url: str, symbols: list[str], mode: str) -> str:
    for symbol in symbols:
        base_url += symbol + '@' + mode + '/'

    return base_url[:-1] + '&timeUnit=MICROSECOND'