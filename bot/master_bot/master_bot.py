import redis
import json
from binance.enums import *
from binance import ThreadedWebsocketManager
from pydantic import BaseModel, Field, ValidationError

class KlineData(BaseModel):
    t: int = Field(..., description="Start time of the kline")
    T: int = Field(..., description="End time of the kline")
    s: str = Field(..., description="Symbol")
    i: str = Field(..., description="Interval")
    o: float = Field(..., description="Open price")
    c: float = Field(..., description="Close price")
    h: float = Field(..., description="High price")
    l: float = Field(..., description="Low price")
    v: float = Field(..., description="Volume")
    x: bool = Field(..., description="Is kline closed")

class KlineMessage(BaseModel):
    E: int = Field(..., description="Event time")
    k: KlineData = Field(..., description="Kline data")

class MasterBot:
    def __init__(self, api_key, api_secret, redis_host, redis_port):
        """
        Initializes the MasterBot with API credentials.
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_client = redis.StrictRedis(host=self.redis_host, port=self.redis_port, decode_responses=True)
        self.twm = ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.api_secret)

    def publish_to_redis(self, topic, msg):
        """
        Publishes kline data to a Redis stream after validation.
        """
        try:
            # Validate message with pydantic
            validated_msg = KlineMessage(**msg)
            kline_data = validated_msg.k

            # Construct the data dictionary
            data = {
                'event_time': validated_msg.E,
                'symbol': kline_data.s,
                'interval': kline_data.i,
                'start_time': kline_data.t,
                'end_time': kline_data.T,
                'open': kline_data.o,
                'close': kline_data.c,
                'high': kline_data.h,
                'low': kline_data.l,
                'volume': kline_data.v,
                'is_closed': kline_data.x
            }

            # Serialize the data as a JSON string
            serialized_data = json.dumps(data)

            # Publish the serialized data to Redis
            self.redis_client.set(topic, serialized_data)

        except ValidationError as e:
            print(f"Validation error for message: {msg} -> {e}", flush=True)


    def handle_kline_message(self, msg, symbol, interval):
        """
        Handles incoming kline messages and publishes them to Redis.
        :param msg: WebSocket message (kline data)
        :param symbol: Symbol (e.g., "BTCUSDT")
        :param interval: Kline interval (e.g., "1m")
        """
        topic = f"{symbol}@{interval}"
        self.publish_to_redis(topic, msg)

    def start_websocket(self, symbol, interval):
        """
        Starts a kline WebSocket for a given symbol and interval.
        :param symbol: Trading pair (e.g., "BTCUSDT")
        :param interval: Interval (e.g., "1m")
        """
        self.twm.start_kline_socket(
            callback=lambda msg: self.handle_kline_message(msg, symbol, interval),
            symbol=symbol,
            interval=interval
        )

    def run(self, pairs):
        """
        Runs the WebSocket manager for all provided pairs and intervals.
        :param pairs: List of tuples [(symbol, interval)]
        """
        self.twm.start()  # Start the WebSocket Manager

        # Start WebSocket for each pair and interval
        for symbol, interval in pairs:
            self.start_websocket(symbol, interval)

        try:
            print("MasterBot is running. Press Ctrl+C to stop.")
            self.twm.join()  # Block the main thread
        except KeyboardInterrupt:
            print("Ctrl+C detected. Stopping the WebSocket Manager...")
        finally:
            self.stop()

    def stop(self):
        """
        Stops the WebSocket manager and closes all connections.
        """
        print("Stopping WebSocket Manager...")
        self.twm.stop()
        print("WebSocket Manager stopped.")

if __name__ == "__main__":
    # Define trading pairs and intervals
    pairs = [("BTCUSDT", "1m"), ("ETHUSDT", "5m")]

    import os
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT")

    # Initialize and run the MasterBot
    bot = MasterBot(api_key, api_secret, redis_host, redis_port)
    bot.run(pairs)