import pandas as pd
import time
from worker_bot import WorkerBot


class CandlePatternBot(WorkerBot):
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
        super().__init__(
            strategy_name,
            pair,
            interval,
            binance_api_key,
            binance_api_secret,
            redis_host,
            redis_port,
        )

    def preprocess_data(self, **kwargs):
        """
        Fetches the last n candles from Binance and updates the class variable `data`.
        This ensures that historical data is available for pattern detection.
        """

        # Extract `n` from kwargs or use a default value
        n = kwargs.get('n', 3)  # Default to 10 candles if not specified

        # Fetch the last n candles
        klines = self.binance_client.get_historical_klines(
            symbol=self.pair, interval=self.interval, limit=n
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
        
        # Update the class variable `data`
        if not hasattr(self, 'data') or self.data is None:
            # Initialize the `data` variable if it doesn't exist
            self.data = candles
        else:
            # Append new data and remove duplicates
            self.data = pd.concat([self.data, candles]).drop_duplicates(subset=['Timestamp'])
            self.data.sort_values(by='Timestamp', inplace=True)
            self.data.reset_index(drop=True, inplace=True)

    def process_data(self, data):
        """
        Updates the class DataFrame `data` with the latest kline information from the socket.
        Ensures historical accuracy by correcting the previous candle when a new one is added.
        """
        # Parse the new candle's start time
        start_time = pd.to_datetime(data['start_time'], unit='ms')
        new_candle = {
            'Timestamp': start_time,
            'Open': float(data['open']),
            'High': float(data['high']),
            'Low': float(data['low']),
            'Close': float(data['close']),
            'Volume': float(data['volume']),
        }

        # Ensure `self.data` is initialized
        if not hasattr(self, 'data') or self.data is None or self.data.empty:
            self.preprocess_data(n=3)

        # Check the last candle's timestamp
        latest_candle_time = self.data.iloc[-1]['Timestamp']

        if latest_candle_time == start_time:
            # Update the existing candle
            self.data.iloc[-1] = new_candle
        elif latest_candle_time < start_time:
            # Append the new candle
            self.data = pd.concat([self.data, pd.DataFrame([new_candle])], ignore_index=True)

            # Correct the previous candle (penultimate) using historical data
            self.correct_previous_candle(latest_candle_time)

        # Deduplicate and sort the DataFrame
        self.data = (
            self.data.drop_duplicates(subset=['Timestamp'], keep='last')
            .sort_values(by='Timestamp')
            .reset_index(drop=True)
        )

        # Debugging: Print the updated DataFrame
        print("Updated DataFrame:", flush=True)
        print(self.data, flush=True)

    def safe_get_historical_klines(self, symbol, interval, limit):
        """
        Safely fetch historical klines with retry logic.
        """
        for attempt in range(5):  # Retry up to 5 times
            try:
                return self.binance_client.get_historical_klines(
                    symbol=symbol, interval=interval, limit=limit
                )
            except Exception as e:
                print(f"Error fetching klines: {e}. Retrying ({attempt + 1}/5)...", flush=True)
                time.sleep(2 ** attempt)  # Exponential backoff
        raise RuntimeError("Failed to fetch historical klines after multiple attempts.")

    def correct_previous_candle(self, candle_time):
        """
        Corrects the penultimate candle by fetching historical data.
        :param candle_time: The timestamp of the candle to be corrected.
        """
        # Fetch the last 2 historical candles (including the one to correct)
        klines = self.binance_client.get_historical_klines(
            symbol=self.pair, interval=self.interval, limit=2, endTime=int(candle_time.timestamp() * 1000)
        )

        # Parse the penultimate candle
        corrected_candle = {
            'Timestamp': pd.to_datetime(klines[-1][0], unit='ms'),
            'Open': float(klines[-1][1]),
            'High': float(klines[-1][2]),
            'Low': float(klines[-1][3]),
            'Close': float(klines[-1][4]),
            'Volume': float(klines[-1][5]),
        }

        # Find the index of the candle to be corrected
        candle_index = self.data[self.data['Timestamp'] == corrected_candle['Timestamp']].index
        if not candle_index.empty:
            self.data.loc[candle_index[0]] = corrected_candle
            print(f"Corrected candle at {corrected_candle['Timestamp']}:", flush=True)
            print(corrected_candle, flush=True)


if __name__ == '__main__':
    bot = CandlePatternBot()
    bot.run()