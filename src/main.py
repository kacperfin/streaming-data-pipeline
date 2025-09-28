from binance_websocket.binance_websocket_client import BinanceWebsocketClient

symbols = ['btcusdt',
           'ethusdt',
           'xrpusdt',
           'bnbusdt',
           'solusdt',
           'dogeusdt',
           'trxusdt',
           'adausdt',
           'wlfiusdt',
           'wbtcusdt',
           'xplusdt',
           'linkusdt',
           'avaxusdt',
           'xlmusdt',
           'bchusdt',
           'suiusdt']

if __name__ == '__main__':
    client = BinanceWebsocketClient(symbols=symbols, mode='aggTrade')
    client.run_forever()