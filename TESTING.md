# Testing Kafka Producer and Consumer

This guide explains how to test the Kafka message flow between the Python producer and Rust consumer.

## Prerequisites

1. **Start Kafka cluster**:
   ```bash
   cd environment
   docker-compose up -d
   ```

2. **Verify Kafka is running**:
   ```bash
   docker-compose ps
   ```
   You should see `kafka`, `zookeeper`, and `kafka-ui` running.

## Testing Steps

### Step 1: Start the Rust Consumer

In one terminal:

```bash
cargo run -p sage_worker
```

You should see:
```
Kafka consumer started successfully, listening for messages...
Running task with request value: 20
Task completed successfully
Running task2 with request value: 20
Task completed successfully
Worker running, press Ctrl+C to exit...
```

The consumer is now waiting for messages.

### Step 2: Run the Python Producer

In another terminal:

```bash
cd samples/producer
source venv/bin/activate  # or use ./venv/bin/python produce.py
python produce.py
```

You should see progress messages:
```
Starting to send 45000 messages...
Sent 0 messages...
Sent 9000 messages...
...
```

### Step 3: Observe Messages

In the consumer terminal, you should now see messages being received:
```
Received: {"Symbol": "IBM", "Price": 102.3, "TimeStamp": 1764365789123}
Received: {"Symbol": "MSFT", "Price": 10.3, "TimeStamp": 1764365799456}
Received: {"Symbol": "GOOG", "Price": 1002.3, "TimeStamp": 1764365809789}
...
```

## Troubleshooting

### Consumer not receiving messages

1. **Check Kafka is accessible**:
   ```bash
   # List topics
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

2. **Verify topic exists**:
   The topic `input_readings` should be listed. If not, create it:
   ```bash
   docker exec kafka kafka-topics --create \
     --bootstrap-server localhost:9092 \
     --topic input_readings \
     --partitions 3 \
     --replication-factor 1
   ```

3. **Check messages in topic**:
   ```bash
   docker exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic input_readings \
     --from-beginning \
     --max-messages 5
   ```

4. **Verify connection settings**:
   - Producer connects to: `localhost:9092`
   - Consumer connects to: `localhost:9092`
   - Both should use the same address

### Producer connection errors

If the producer can't connect:

1. Check Kafka is listening on localhost:9092:
   ```bash
   netstat -an | grep 9092
   ```

2. Check Kafka logs:
   ```bash
   docker logs kafka
   ```

## Using Kafka UI

Access the web interface at http://localhost:8080 to:
- View topics and their messages
- Monitor consumer groups
- Check broker health
- Inspect message details

## Configuration Summary

### Rust Consumer (sage_worker)
- Bootstrap servers: `localhost:9092`
- Topic: `input-readings`
- Consumer group: `andean-group`
- Auto offset reset: `earliest` (reads from beginning)

### Python Producer (samples/producer)
- Bootstrap servers: `localhost:9092`
- Topic: `input-readings`
- Sends 18 messages (2 iterations × 9 stocks)

## Stopping

1. **Stop consumer**: Press `Ctrl+C` in the consumer terminal
2. **Stop Kafka**: 
   ```bash
   cd environment
   docker-compose down
