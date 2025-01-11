import redis
import json
import logging

from binance.lib.utils import config_logging
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient

from pydantic import BaseModel, Field, ValidationError

config_logging(logging, logging.DEBUG)


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
    def __init__(
        self,
        api_key,
        api_secret,
        redis_host,
        redis_port,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.redis_host = redis_host
        self.redis_port = redis_port

        self.redis_client = redis.StrictRedis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )

        self.client = SpotWebsocketStreamClient(
            on_message=self.message_handler, is_combined=True
        )
        self.streams = []

    def message_handler(self, _, message):
        # Convert the JSON string to a python dict
        message_dict = json.loads(message)

        logging.debug(message)

        # Create topic name
        topic = message_dict.get("stream", "")
        if len(topic) == 0:
            logging.error("No stream name found in message")
            return

        # Extract the kline message
        msg = message_dict.get("data", {})
        if not msg:
            logging.error("No message data in message")
            return

        # Publish to Redis
        self.publish_to_redis(topic, msg)

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
                "event_time": validated_msg.E,
                "symbol": kline_data.s,
                "interval": kline_data.i,
                "start_time": kline_data.t,
                "end_time": kline_data.T,
                "open": kline_data.o,
                "close": kline_data.c,
                "high": kline_data.h,
                "low": kline_data.l,
                "volume": kline_data.v,
                "is_closed": kline_data.x,
            }

            # Serialize the data as a JSON string
            serialized_data = json.dumps(data)

            # Publish the serialized data to Redis
            self.redis_client.set(topic, serialized_data)

        except ValidationError as e:
            logging.error(f"Validation error for message: {msg} -> {e}")

    def add_stream(self, symbol: str, interval: str):
        stream = f"{symbol.lower()}@kline_{interval.lower()}"

        self.streams.append(stream)

        self.client.subscribe(stream)


if __name__ == "__main__":
    import os

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    redis_host = os.getenv("REDIS_HOST")
    redis_port = os.getenv("REDIS_PORT")

    mb = MasterBot(
        api_key=api_key,
        api_secret=api_secret,
        redis_host=redis_host,
        redis_port=redis_port,
    )

    mb.add_stream("BTCUSDT", "1m")
