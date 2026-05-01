import boto3
import json
import random
import time
import uuid
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

kinesis = boto3.client('kinesis', region_name='us-east-1')

STREAM_NAME = 'clickstream'

EVENT_TYPES = ['click', 'add_to_cart', 'purchase', 'view']
DEVICES = ['desktop', 'mobile', 'tablet']
REFERRERS = ['google', 'facebook', 'direct', 'email', 'twitter']

def generate_event():
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': f"user_{random.randint(1, 10000)}",
        'event_type': random.choice(EVENT_TYPES),
        'product_id': f"prod_{random.randint(1, 1000)}",
        'timestamp': datetime.utcnow().isoformat(),
        'session_id': str(uuid.uuid4()),
        'device': random.choice(DEVICES),
        'referrer': random.choice(REFERRERS)
    }

def put_event(event):
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event),
        PartitionKey=event['user_id']  # Same user = same shard
    )

if __name__ == "__main__":
    print("Starting clickstream producer...")
    print("Press Ctrl+C to stop")
    
    count = 0
    try:
        while True:
            event = generate_event()
            put_event(event)
            count += 1
            
            if count % 100 == 0:
                print(f"Sent {count} events...")
            
            # ~1000 events/minute = ~17 events/second
            time.sleep(0.06)
            
    except KeyboardInterrupt:
        print(f"\nStopped. Total events sent: {count}")