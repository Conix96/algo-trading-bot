import redis

BINANCE_API_KEY="xDB5FIZHEaodt0lbOtQieDB43DjzAvzKHO7Mnt1vK9Yu8UuqU4iYsajx2YABhSve"
BINANCE_API_SECRET="ipHcFpm5xqPxTNPVj2vK6mRC5TnGtvkcxYItqqhhzgBMXJrqGV0dTByka5ylFEsn"


# redis_client = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)

# topic="BTCUSDT@1m"
# message = redis_client.get(topic)
# print(message)

# from candle_pattern import CandlePatternBot

# bot = CandlePatternBot("Toto", "BTCUSDT", "1h", BINANCE_API_KEY, BINANCE_API_SECRET)

# bot.preprocess_data()

# print(bot.data)

# Fetch the last 3 candles

from binance import Client
import pandas as pd

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

klines = client.get_historical_klines(
    symbol="BTCUSDT", interval="1m", limit=10
)

# Convert the data to a Pandas DataFrame
columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
candles = pd.DataFrame(
    klines, columns=columns + ['CloseTime', 'QuoteAssetVolume', 
                            'NumberOfTrades', 'TakerBuyBaseAssetVolume', 
                            'TakerBuyQuoteAssetVolume', 'Ignore']
)

# Keep only the relevant columns and convert types
candles = candles[columns]
candles['Timestamp'] = pd.to_datetime(candles['Timestamp'], unit='ms')
candles[['Open', 'High', 'Low', 'Close', 'Volume']] = candles[
    ['Open', 'High', 'Low', 'Close', 'Volume']
].astype(float)

print(candles)