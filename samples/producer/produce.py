from confluent_kafka import Producer
import json
import datetime
import time
import random
import uuid

input_data = [
    {"task_name": "PrimeTask", "task_context": "{\"id\":\"{id1}\",\"limit\":45000}"},
    {"task_name": "PrimeTask", "task_context": "{\"id\":\"{id2}\",\"limit\":35000}"},
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
            # Generate a new UUID for each message
            task_id = str(uuid.uuid4())
            
            # Replace the placeholder with actual UUID
            task_context = entry["task_context"].replace("{id1}", task_id).replace("{id2}", task_id)
            
            message = {
                "task_name": entry["task_name"],
                "task_context": task_context,
                "TimeStamp": ts + random.randint(1000, 10000000)
            }
            
            output = json.dumps(message).encode('utf-8')
            
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
