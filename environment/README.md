# Kafka Development Environment

This Docker Compose setup provides a complete Kafka cluster for local development.

## Components

- **Zookeeper**: Coordination service for Kafka (port 2181)
- **Kafka**: Message broker (ports 9092 for external access, 9093 for internal)
- **Kafka UI**: Web interface for managing and monitoring Kafka (port 8080)

## Prerequisites

- Docker
- Docker Compose

## Starting the Cluster

```bash
cd environment
docker-compose up -d
```

## Stopping the Cluster

```bash
docker-compose down
```

## Accessing the Services

- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: http://localhost:8080
- **Zookeeper**: `localhost:2181`

## Connecting from Applications

Use the following connection string in your applications:
```
localhost:9092
```

For applications running inside Docker containers on the same network, use:
```
kafka:9093
```

## Useful Commands

### Check running containers
```bash
docker-compose ps
```

### View logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
```

### Create a topic manually
```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --partitions 3 \
  --replication-factor 1
```

### List topics
```bash
docker exec -it kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

### Produce messages
```bash
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

### Consume messages
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning
```

## Cleanup

To remove all containers and volumes:
```bash
docker-compose down -v
