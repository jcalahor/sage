# Testing Sage with Kafka

This guide explains how to test the complete Sage task queue system with Python producers and consumers.

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

## Complete Testing Flow

### Step 1: Start the Rust Worker

In terminal 1:

```bash
cargo run --bin sage_worker
```

You should see:
```
Kafka consumer started successfully, listening for messages...
Worker running, press Ctrl+C to exit...
```

The worker is now waiting for task requests.

### Step 2: Run the Python Producer (Send Tasks)

In terminal 2:

```bash
cd samples/producer
source venv/bin/activate
python produce.py
```

You should see:
```
Starting to send 4 messages...
Message delivered to input-readings [0]
Flushing remaining messages...
Successfully sent all 4 messages!
```

### Step 3: Observe Worker Processing

Back in terminal 1 (worker), you should see tasks being executed:

```
Received SageMessage: task_name='PrimeTask', task_context='{"id":"abc-123","limit":45000}'
Task 'PrimeTask' completed successfully (spawn_blocking)
Rayon parallel: Found 4669 primes in 23.5ms for array of 45000 items
PrimeTask Response (ID: abc-123): PrimeTaskResponseData { prime_founds: 4669 }
Response sent successfully to topic 'responses': {"id":"abc-123","prime_founds":4669}
```

### Step 4: Run the Python Consumer (Receive Results)

In terminal 3:

```bash
cd samples/consumer
source venv/bin/activate
python consume.py
```

You should see responses with correlated IDs:

```
Starting to consume messages from 'responses' topic...
Note: Response messages now include an 'id' field from the request
Press Ctrl+C to stop

[Message #1]
  Topic: responses
  Partition: 0
  Offset: 0
  Key: PrimeTask
  Value: {
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "prime_founds": 4669
}
------------------------------------------------------------
```

## What's Happening

1. **Producer** sends task requests with auto-generated UUIDs:
   ```json
   {
     "task_name": "PrimeTask",
     "task_context": "{\"id\":\"abc-123\",\"limit\":45000}"
   }
   ```

2. **Worker** receives, processes, and returns results with same ID:
   ```json
   {
     "id": "abc-123",
     "prime_founds": 4669
   }
   ```

3. **Consumer** receives responses and can correlate them with requests via ID

## Message Flow Diagram

```
Producer → [input-readings] → Worker → [responses] → Consumer
   |              ↓              |          ↓           |
   |         Task Request        |    Task Response    |
   |      {"id":"abc-123",...}   |  {"id":"abc-123"..} |
   └────────────────────────────┴───────────────────────┘
              Same UUID for correlation!
```

## Troubleshooting

### Worker not receiving messages

1. **Check topics exist**:
   ```bash
   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```
   Should show `input-readings` and `responses`.

2. **Create topics if missing**:
   ```bash
   # Input topic
   docker exec kafka kafka-topics --create \
     --bootstrap-server localhost:9092 \
     --topic input-readings \
     --partitions 3 \
     --replication-factor 1

   # Response topic
   docker exec kafka kafka-topics --create \
     --bootstrap-server localhost:9092 \
     --topic responses \
     --partitions 3 \
     --replication-factor 1
   ```

3. **Check messages in topic**:
   ```bash
   # Check input messages
   docker exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic input-readings \
     --from-beginning \
     --max-messages 5

   # Check response messages
   docker exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic responses \
     --from-beginning \
     --max-messages 5
   ```

### Connection errors

1. **Verify Kafka is accessible**:
   ```bash
   netstat -an | grep 9092
   # Should show LISTEN on port 9092
   ```

2. **Check Kafka logs**:
   ```bash
   docker logs kafka
   ```

3. **Verify Python dependencies**:
   ```bash
   cd samples/producer
   source venv/bin/activate
   pip list | grep confluent
   ```
   Should show `confluent-kafka`.

### No responses in consumer

1. **Check worker is processing**:
   - Look for "Task completed successfully" in worker logs
   - Verify "Response sent successfully" appears

2. **Check consumer group offset**:
   ```bash
   docker exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --describe \
     --group python-consumer-group
   ```

## Using Kafka UI

Access the web interface at **http://localhost:8080** to:
- View `input-readings` and `responses` topics
- Inspect message payloads and IDs
- Monitor consumer groups (`andean-group`, `python-consumer-group`)
- Check broker health
- View message flow in real-time

## Configuration Summary

### Rust Worker (sage_worker)
- **Consumes from**: `input-readings` topic
- **Produces to**: `responses` topic
- **Consumer group**: `andean-group`
- **Auto offset reset**: `earliest`
- **Bootstrap servers**: `localhost:9092`

### Python Producer (samples/producer)
- **Produces to**: `input-readings` topic
- **Message count**: 4 (2 iterations × 2 task configs)
- **Bootstrap servers**: `localhost:9092`
- **Auto-generates**: UUID for each task

### Python Consumer (samples/consumer)
- **Consumes from**: `responses` topic
- **Consumer group**: `python-consumer-group`
- **Auto offset reset**: `earliest`
- **Bootstrap servers**: `localhost:9092`

## Testing Different Task Types

To test with custom limits:

1. **Edit producer** (`samples/producer/produce.py`):
   ```python
   input_data = [
       {"task_name": "PrimeTask", "task_context": "{\"id\":\"{id1}\",\"limit\":100000}"},
       {"task_name": "SampleTask", "task_context": "{\"id\":\"{id2}\",\"limit\":1000}"},
   ]
   ```

2. **Run producer** and watch worker process different limits

3. **Observe** CPU-intensive vs quick tasks:
   - `PrimeTask` uses `spawn_blocking` (parallel)
   - `SampleTask` is quick (returns 0 primes)

## Stopping

1. **Stop consumer**: `Ctrl+C` in consumer terminal
2. **Stop worker**: `Ctrl+C` in worker terminal  
3. **Stop Kafka**:
   ```bash
   cd environment
   docker-compose down
   ```

## Performance Testing

To test high throughput:

1. **Increase message count** in producer:
   ```python
   LIMIT = 1000  # Send 2000 messages
   ```

2. **Run multiple workers**:
   ```bash
   # Terminal 1
   cargo run --bin sage_worker
   
   # Terminal 2
   cargo run --bin sage_worker
   
   # Terminal 3
   cargo run --bin sage_worker
   ```

3. **Send tasks** and observe load distribution across workers

4. **Monitor** in Kafka UI to see message distribution across partitions

---

**Tip**: Use `Ctrl+C` gracefully shuts down all components with proper cleanup!
