from confluent_kafka import Producer
import json
import datetime
import time
import random

input_data = [
    {"Symbol": "IBM", "Price": 102.3},
    {"Symbol": "MSFT", "Price": 10.3},
    {"Symbol": "GOOG", "Price": 1002.3},
    {"Symbol": "META", "Price": 299.3},
    {"Symbol": "XYZ", "Price": 211.3},
    {"Symbol": "NVDA", "Price": 112.3},
    {"Symbol": "APPL", "Price": 233.3},
    {"Symbol": "LKM", "Price": 11.3},
    {"Symbol": "UTC", "Price": 33.3},
]


def delivery_report(err, msg):
    """Callback function for message delivery reports"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def run():
    LIMIT = 2
    
    # Create Producer instance
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'python-producer'
    }
    producer = Producer(conf)
    
    print(f"Starting to send {LIMIT * len(input_data)} messages...")
    
    for i in range(LIMIT):
        ts = int(datetime.datetime.now().timestamp())
        for entry in input_data:
            entry["TimeStamp"] = ts + random.randint(1000, 10000000)
            output = json.dumps(entry).encode('utf-8')
            
            # Produce message
            producer.produce(
                'input-readings',
                value=output,
                callback=delivery_report if i % 1000 == 0 else None
            )
            
        # Trigger any available delivery report callbacks
        producer.poll(0)
        
        if i % 1000 == 0:
            print(f"Sent {i * len(input_data)} messages...")
    
    # Wait for any outstanding messages to be delivered
    print("Flushing remaining messages...")
    producer.flush()
    print(f"Successfully sent all {LIMIT * len(input_data)} messages!")


if __name__ == "__main__":
    run()
