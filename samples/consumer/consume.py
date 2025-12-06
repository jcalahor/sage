from confluent_kafka import Consumer, KafkaError
import json
import signal
import sys


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\nShutting down consumer...')
    sys.exit(0)


def run():
    # Create Consumer instance
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'python-consumer-group',
        'client.id': 'python-consumer',
        'auto.offset.reset': 'earliest',  # Start from beginning if no offset
        'enable.auto.commit': True,
    }
    consumer = Consumer(conf)
    
    # Subscribe to topic
    consumer.subscribe(['responses'])
    
    print("Starting to consume messages from 'responses' topic...")
    print("Press Ctrl+C to stop\n")
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    message_count = 0
    
    try:
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message available within timeout
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'Reached end of partition {msg.partition()}')
                else:
                    print(f'Error: {msg.error()}')
                continue
            
            # Message successfully received
            message_count += 1
            
            # Decode and parse the message
            try:
                value = msg.value().decode('utf-8')
                data = json.loads(value)
                
                print(f"[Message #{message_count}]")
                print(f"  Topic: {msg.topic()}")
                print(f"  Partition: {msg.partition()}")
                print(f"  Offset: {msg.offset()}")
                print(f"  Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
                print(f"  Value: {json.dumps(data, indent=2)}")
                print("-" * 60)
                
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON: {e}")
                print(f"Raw value: {msg.value()}")
            except Exception as e:
                print(f"Error processing message: {e}")
    
    finally:
        # Close the consumer
        print("\nClosing consumer...")
        consumer.close()
        print(f"Total messages consumed: {message_count}")


if __name__ == "__main__":
    run()
