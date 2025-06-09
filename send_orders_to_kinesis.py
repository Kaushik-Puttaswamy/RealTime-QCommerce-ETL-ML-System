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

# Generate users and micro-fulfillment zones
def generate_users(n=200):
    return [{
        'user_id': faker.unique.random_int(1000, 9999),
        'location': faker.city(),
        'coordinates': {
            'lat': float(faker.latitude()),
            'lon': float(faker.longitude())
        },
        'age': random.randint(18, 60),
        'gender': random.choice(['Male', 'Female', 'Other']),
        'loyalty_score': round(random.uniform(0.1, 0.95), 2),
        'is_subscribed': random.choice([True, False])
    } for _ in range(n)]

def generate_inventory(n=100):
    categories = ['Snacks', 'Beverages', 'Grocery', 'Frozen', 'Personal Care', 'Pet Care']
    return [{
        'item_id': i + 1,
        'item_name': faker.word().capitalize(),
        'category': random.choice(categories),
        'stock_level': random.randint(20, 200),
        'price': round(random.uniform(2, 25), 2),
        'fulfillment_center': faker.city()
    } for i in range(n)]

# Generate Q-commerce orders for a specific day
def generate_qcommerce_orders(users, inventory, date, n=600):
    orders = []
    for _ in range(n):
        user = random.choice(users)
        num_items = random.randint(1, 3)
        selected_items = random.sample(inventory, num_items)
        timestamp = datetime.combine(date, datetime.min.time()) + timedelta(minutes=random.randint(0, 1439))
        total_amount = sum([round(item['price'] * random.uniform(1, 2), 2) for item in selected_items])
        delivery_eta = timestamp + timedelta(minutes=random.randint(10, 40))
        order = {
            'order_id': str(uuid.uuid4()),
            'timestamp': timestamp.isoformat(),
            'user_id': user['user_id'],
            'user_info': user,
            'items': [{
                'item_id': item['item_id'],
                'item_name': item['item_name'],
                'category': item['category'],
                'price': item['price'],
                'quantity': random.randint(1, 2)
            } for item in selected_items],
            'total_amount': round(total_amount, 2),
            'delivery_address': faker.address().replace('\n', ', '),
            'delivery_eta': delivery_eta.isoformat(),
            'delivery_status': random.choice(['Rider Assigned', 'Picked Up', 'Delivered', 'Cancelled']),
            'rider': {
                'rider_id': faker.random_int(5000, 9000),
                'name': faker.first_name(),
                'contact': faker.phone_number()
            },
            'fulfillment_center': selected_items[0]['fulfillment_center']
        }
        orders.append(order)
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

    start_date = datetime.now() - timedelta(days=25)
    print(f"🛵 Sending 15,000 Q-commerce orders (600/day × 25 days) to Kinesis stream: {stream_name}\n")

    for i in range(25):
        day = start_date + timedelta(days=i)
        daily_orders = generate_qcommerce_orders(users, inventory, day, n=600)

        for order in daily_orders:
            response = send_to_kinesis(stream_name, order, order['user_id'])
            print(f"📦 {day.date()} - Sent OrderID: {order['order_id']} | Seq#: {response['SequenceNumber']}")
            time.sleep(0.02)  # Can tweak based on speed you want

    print("\n✅ All Q-commerce orders sent to AWS Kinesis!")