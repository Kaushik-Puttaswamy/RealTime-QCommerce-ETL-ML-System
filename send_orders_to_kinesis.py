import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
import boto3
import json
import time

faker = Faker()

# AWS Kinesis configuration
kinesis = boto3.client('kinesis', region_name='eu-north-1')
stream_name = 'qcommerce-stream'

# Generate users and inventory
def generate_users(n=100):
    return [{
        'user_id': faker.unique.random_int(1000, 9999),
        'location': faker.city(),
        'age': random.randint(18, 65),
        'gender': random.choice(['Male', 'Female', 'Other']),
        'loyalty_score': round(random.uniform(0, 1), 2)
    } for _ in range(n)]

def generate_inventory(n=50):
    categories = ['Grocery', 'Electronics', 'Snacks', 'Beverages', 'Personal Care']
    return [{
        'item_id': i + 1,
        'item_name': faker.word().capitalize(),
        'category': random.choice(categories),
        'stock_level': random.randint(0, 100),
        'price': round(random.uniform(1, 100), 2)
    } for i in range(n)]

def generate_orders(users, inventory, n=100):
    orders = []
    for _ in range(n):
        user = random.choice(users)
        item = random.choice(inventory)
        timestamp = datetime.now() - timedelta(minutes=random.randint(0, 1440))
        orders.append({
            'order_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'item_id': item['item_id'],
            'timestamp': timestamp.isoformat(),
            'total_amount': round(item['price'] * random.uniform(1, 3), 2),
            'delivery_address': faker.address().replace('\n', ', ')
        })
    return orders

# Send single event to Kinesis
def send_to_kinesis(stream_name, data, partition_key):
    response = kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=str(partition_key)
    )
    return response

# Main
if __name__ == "__main__":
    users = generate_users()
    inventory = generate_inventory()
    orders = generate_orders(users, inventory)

    print(f"🚀 Sending {len(orders)} mock orders to Kinesis stream: {stream_name}\n")

    for order in orders:
        response = send_to_kinesis(stream_name, order, order['user_id'])
        print(f"✅ Sent OrderID: {order['order_id']} | Seq#: {response['SequenceNumber']}")
        time.sleep(0.1)  # Simulate event spacing (optional)

    print("\n🎉 All mock orders sent to AWS Kinesis!")