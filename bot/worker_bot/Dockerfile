FROM python:3.9-slim

# Set a working directory
WORKDIR /worker

# Copy the requirements file and install dependencies
COPY ./bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the worker bot code
COPY ./bot/worker_bot /worker

# Set environment variables for dynamic configuration (optional)
ENV STRATEGY_NAME="default"

# Command to run the worker bot
CMD ["python", "candle_pattern.py"]
