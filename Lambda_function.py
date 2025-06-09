import json
import boto3
import base64
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('OrdersTable')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decode Base64 and parse JSON with Decimal
            payload = json.loads(
                base64.b64decode(record['kinesis']['data']).decode('utf-8'),
                parse_float=Decimal
            )

            print(f"🔍 Processing order: {payload.get('order_id')}")

            # Ensure keys are properly formatted for DynamoDB (no floats, all strings/Decimals)
            item_to_store = {
                'order_id': payload['order_id'],
                'timestamp': payload['timestamp'],
                'user_id': str(payload['user_id']),
                'user_info': payload['user_info'],
                'items': payload['items'],
                'total_amount': Decimal(str(payload['total_amount'])),
                'delivery_address': payload['delivery_address'],
                'delivery_eta': payload['delivery_eta'],
                'delivery_status': payload['delivery_status'],
                'rider': payload['rider'],
                'fulfillment_center': payload['fulfillment_center']
            }

            table.put_item(Item=item_to_store)

        except Exception as e:
            print(f"❌ Error processing record: {e}")
            continue

    return {
        'statusCode': 200,
        'body': json.dumps('Processed records successfully')
    }