# Kafka Producer Sample

This is a sample Python script that produces messages to a Kafka topic.

## Setup

### 1. Create and activate virtual environment

```bash
# Create virtual environment (already done)
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate
```

### 2. Install dependencies

```bash
# Install from requirements file
pip install -r requirements.txt
```

## Running the Producer

### Option 1: Using the virtual environment directly

```bash
./venv/bin/python produce.py
```

### Option 2: After activating virtual environment

```bash
source venv/bin/activate
python produce.py
```

## Configuration

The producer script:
- Uses `confluent-kafka` library (compatible with Python 3.12)
- Connects to Kafka at `localhost:9092` (works from host machine)
- Sends messages to the `input-readings` topic
- Generates task requests with unique UUIDs for each message
- Sends PrimeTask requests with different limit values (45000 and 35000)
- Each message includes:
  - `task_name`: The task to execute (e.g., "PrimeTask")
  - `task_context`: JSON string with `id` (UUID) and task-specific data
  - `TimeStamp`: Message timestamp
- Automatically generates a unique UUID v4 for each request
- Includes delivery callbacks for monitoring

## Prerequisites

Make sure your Kafka cluster is running:

```bash
cd ../../environment
docker-compose up -d
```

## Connecting to Kafka

The script is configured to connect to `localhost:9092`, which works when:
- Running from the host machine with Kafka exposed on port 9092
- The Kafka cluster in `environment/` is running

If you need to connect to a different Kafka broker, edit the `bootstrap.servers` configuration in `produce.py`.

## Output

The script will print progress every 2000 messages sent.
