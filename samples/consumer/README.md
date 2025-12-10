# Kafka Consumer for Task Responses

This Python script consumes messages from the Kafka `responses` topic, which contains task execution results from the Sage worker.

## Prerequisites

- Python 3.6+
- Kafka running on `localhost:9092`
- confluent-kafka Python library

## Installation

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the consumer:

```bash
python consume.py
```

The consumer will:
- Connect to Kafka at `localhost:9092`
- Subscribe to the `responses` topic
- Display all incoming messages with their metadata (topic, partition, offset, key)
- Parse and pretty-print the JSON response payload
- Show the task execution results including the request ID for traceability

Press `Ctrl+C` to gracefully shutdown the consumer.

## Message Format

The consumer expects JSON messages containing task responses. Each response includes the original request ID for tracking:

Example response:
```json
{
  "prime_founds": 1234
}
```

Note: The response messages correspond to requests that include an `id` field (UUID) for tracking and correlation.

## Configuration

You can modify the consumer configuration in `consume.py`:

- `bootstrap.servers`: Kafka broker address (default: `localhost:9092`)
- `group.id`: Consumer group name (default: `python-consumer-group`)
- `auto.offset.reset`: Where to start reading (default: `earliest`)

## Example Output

```
Starting to consume messages from 'responses' topic...
Press Ctrl+C to stop

[Message #1]
  Topic: responses
  Partition: 0
  Offset: 42
  Key: PrimeTask
  Value: {
  "prime_founds": 4669
}
------------------------------------------------------------
```
