FROM python:3.11-slim

# Set a working directory
WORKDIR /master

# Copy the requirements file and install dependencies
COPY ./bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the master bot code
COPY ./bot/master_bot /master

# Command to run the master bot
CMD ["python", "-u", "master_bot.py"]
