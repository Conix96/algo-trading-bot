import os
import json
import redis
import time
from pydantic import BaseModel, Field, ValidationError
from binance import Client

# Define a Pydantic model for the consumed data
class ConsumedData(BaseModel):
    start_time: int = Field(..., description="Start time of the kline")
    open: float = Field(..., description="Open price")
    close: float = Field(..., description="Close price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    volume: float = Field(..., description="Volume")
    is_closed: bool = Field(..., description="Is kline closed")

class WorkerBot:
    def __init__(
        self,
        strategy_name=None,
        pair=None,
        interval=None,
        binance_api_key=None,
        binance_api_secret=None,
        redis_host=None,
        redis_port=None,
    ):
        self.strategy_name = strategy_name or os.getenv("STRATEGY_NAME")
        self.pair = pair or os.getenv("PAIR")
        self.interval = interval or os.getenv("INTERVAL")
        self.binance_api_key = binance_api_key or os.getenv("BINANCE_API_KEY")
        self.binance_api_secret = binance_api_secret or os.getenv("BINANCE_API_SECRET")
        self.redis_host = redis_host or os.getenv("REDIS_HOST")
        self.redis_port = redis_port or os.getenv("REDIS_PORT")

        self.topic = f"{self.pair}@{self.interval}"
        self.binance_client = Client(self.binance_api_key, self.binance_api_secret)
        self.redis_client = redis.StrictRedis(
            host=self.redis_host, port=self.redis_port, decode_responses=True
        )

    def run(self):
        self.preprocess_data()
        self.consume()

    def preprocess_data(self, **kwargs):
        """
        Base method for preprocessing data. Designed to be overridden by child classes.
        """
        pass

    def process_data(self, data):
        """
        Processes the data based on the strategy logic.
        :param data: Parsed data from the Redis stream.
        """
        print(f"Open: {data['open']}, Close: {data['close']}, High: {data['high']}, Low: {data['low']}", flush=True)
        pass  # TODO: Update the current bar, and check if it is an entry.

    def consume(self):
        """
        Consumes messages from the Redis stream and processes the data.
        """
        while True:
            # Fetch the latest value from the Redis key
            serialized_data = self.redis_client.get(self.topic)

            if serialized_data:
                # Deserialize the JSON string
                parsed_data = json.loads(serialized_data)
                # Validate the data using Pydantic
                validated_data = ConsumedData(**parsed_data)
                # Pass the validated data to the processor
                self.process_data(validated_data.dict())
                time.sleep(1)


    # def consume(self):
    #     """
    #     Consumes messages from the Redis stream and processes the data.
    #     """
    #     while True:
    #         try:
    #             # Fetch the latest value from the Redis key
    #             serialized_data = self.redis_client.get(self.topic)

    #             if serialized_data:
    #                 # Deserialize the JSON string
    #                 try:
    #                     parsed_data = json.loads(serialized_data)
    #                 except json.JSONDecodeError:
    #                     print("Error decoding JSON data. Skipping message.", flush=True)
    #                     time.sleep(1)
    #                     continue

    #                 # Validate the data using Pydantic
    #                 try:
    #                     validated_data = ConsumedData(**parsed_data)
    #                     # Pass the validated data to the processor
    #                     self.process_data(validated_data.dict())
    #                 except ValidationError as e:
    #                     print(f"Validation error: {e.json()}. Skipping message.", flush=True)
    #                     time.sleep(1)
    #                     continue
    #             else:
    #                 print("No data received from Redis. Retrying...", flush=True)
    #                 time.sleep(1)

    #         except Exception as e:
    #             print(f"Error in consume: {e}", flush=True)
    #             time.sleep(1)


if __name__ == "__main__":
    bot = WorkerBot()
    bot.consume()
