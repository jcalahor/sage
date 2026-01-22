# Configuration Guide

This document describes all configuration options for the Sage task scheduling system.

## Environment Variables

All configuration is managed through environment variables. You can set these in a `.env` file in the project root or export them directly in your shell.

### Quick Start

1. Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual configuration values

3. The `.env` file will be automatically loaded when you run the server or worker

### Configuration Options

#### Database Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgres://sage:sage_password@localhost:5432/sage_db` |
| `DB_MAX_CONNECTIONS` | Maximum database connection pool size | `5` |

#### Kafka Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses (comma-separated) | `localhost:9092` |
| `KAFKA_SERVER_GROUP_ID` | Consumer group ID for the server | `sage_server` |
| `KAFKA_WORKER_GROUP_ID` | Consumer group ID for workers | `andean-group` |

#### Kafka Topics

| Variable | Description | Example |
|----------|-------------|---------|
| `KAFKA_TOPIC_INPUT` | Topic for incoming task requests | `input-readings` |
| `KAFKA_TOPIC_RESPONSES` | Topic for task responses | `responses` |
| `KAFKA_TOPIC_ERRORS` | Topic for task errors | `task-errors` |

#### Server Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `SERVER_HOST` | Server bind address | `0.0.0.0` |
| `SERVER_PORT` | Server port | `4000` |

#### Scheduler Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `SCHEDULER_INTERVAL_SECONDS` | Interval for checking scheduled tasks (in seconds) | `30` |

## Running the Applications

### Sage Server

```bash
cd sage_server
cargo run
```

The server will:
- Load environment variables from `.env`
- Connect to PostgreSQL database
- Connect to Kafka brokers
- Start the HTTP API server
- Start the task scheduler
- Start consuming responses and errors from Kafka

### Sage Worker

```bash
cd sage_worker
cargo run
```

The worker will:
- Load environment variables from `.env`
- Connect to Kafka brokers
- Start consuming tasks from the input topic
- Process tasks and send results to response/error topics

## Docker Compose

If you're using the provided Docker Compose setup in the `environment` directory, the default values in `.env` will work out of the box for local development.

## Production Considerations

For production deployments:

1. **Database**: Use a production-grade PostgreSQL instance with appropriate backup and replication
2. **Kafka**: Use a Kafka cluster with multiple brokers for high availability
3. **Security**: 
   - Use strong passwords in `DATABASE_URL`
   - Consider using TLS/SSL for Kafka and PostgreSQL connections
   - Restrict `SERVER_HOST` to specific interfaces if needed
4. **Scaling**: 
   - Adjust `DB_MAX_CONNECTIONS` based on your workload
   - Run multiple worker instances with the same `KAFKA_WORKER_GROUP_ID` for load distribution
5. **Monitoring**: Set up monitoring for Kafka lag, database connections, and task processing times

## Troubleshooting

### "Environment variable not set" errors

If you see errors like `DATABASE_URL must be set in environment or .env file`:
1. Ensure your `.env` file exists in the project root
2. Verify all required variables are set
3. Check for typos in variable names

### Connection failures

- **PostgreSQL**: Verify the database is running and the `DATABASE_URL` is correct
- **Kafka**: Ensure Kafka brokers are accessible at `KAFKA_BOOTSTRAP_SERVERS`
- Check firewall rules and network connectivity

### Topics not found

Ensure Kafka topics exist before starting the applications. You can create them manually or configure Kafka to auto-create topics.
