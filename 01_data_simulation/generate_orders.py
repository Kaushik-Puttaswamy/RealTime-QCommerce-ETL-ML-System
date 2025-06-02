import json, random, time
from datetime import datetime

items = ['milk', 'bread', 'soda', 'eggs', 'rice']
locations = ['Zone-A', 'Zone-B', 'Zone-C']

def generate_order():
    return {
        "order_id": random.randint(1000, 9999),
        "customer_id": random.randint(1, 100),
        "timestamp": datetime.now().isoformat(),
        "items": random.sample(items, k=random.randint(1, 3)),
        "total_price": round(random.uniform(5, 50), 2),
        "location": random.choice(locations)
    }

while True:
    order = generate_order()
    with open("orders.json", "a") as f:
        f.write(json.dumps(order) + "\n")
    time.sleep(2)