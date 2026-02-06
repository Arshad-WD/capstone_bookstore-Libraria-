import boto3
import csv
import io
import os
from decimal import Decimal
from werkzeug.security import generate_password_hash
from botocore.exceptions import ClientError

# --- Configuration (Sync with app_aws.py) ---
AWS_REGION = "us-east-1"
S3_BUCKET = "bookbazaar-assets-861276080904"
TABLE_USERS = "BookBazaarUsers"
TABLE_BOOKS = "BookBazaarBooks"
TABLE_ORDERS = "BookBazaarOrders"

# --- Clients ---
s3_client = boto3.client('s3', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def check_s3_availability():
    """Check if we can actually use S3."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"[MODE] S3 Pipe: Detected bucket '{S3_BUCKET}'. Using S3 pipeline.")
        return True
    except Exception:
        print(f"[MODE] Direct Path: S3 Bucket '{S3_BUCKET}' not reachable. Using direct local ingestion.")
        return False

def get_csv_reader(local_path, s3_key, use_s3):
    """Retrieve data source based on available mode."""
    if use_s3:
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            return csv.DictReader(io.StringIO(content))
        except Exception as e:
            print(f"  [ERROR] S3 Read failed: {e}. Falling back to local.")
    
    if os.path.exists(local_path):
        return csv.DictReader(open(local_path, 'r', encoding='utf-8'))
    return None

def process_users(data_dir, use_s3):
    print("\n--- Syncing Users ---")
    table = dynamodb.Table(TABLE_USERS)
    reader = get_csv_reader(os.path.join(data_dir, 'users.csv'), "imports/users.csv", use_s3)
    user_map = {}
    
    if not reader: 
        print("  [ERROR] User data source not found.")
        return {}

    with table.batch_writer() as batch:
        for i, row in enumerate(reader, 1):
            user_id = f"u{i}"
            item = {
                'id': user_id,
                'username': row['username'],
                'email': row['email'],
                'role': row['role'],
                'is_validated': row['is_validated'].lower() == 'true',
                'password_hash': generate_password_hash(row['password'])
            }
            batch.put_item(Item=item)
            user_map[row['username']] = user_id
            print(f"  Synced: {row['username']}")
    return user_map

def process_books(data_dir, user_map, use_s3):
    print("\n--- Syncing Books ---")
    table = dynamodb.Table(TABLE_BOOKS)
    reader = get_csv_reader(os.path.join(data_dir, 'books.csv'), "imports/books.csv", use_s3)
    book_map = {}
    
    if not reader: return {}

    with table.batch_writer() as batch:
        for i, row in enumerate(reader, 1):
            book_id = f"b{i}"
            item = {
                'id': book_id,
                'title': row['title'],
                'author': row['author'],
                'description': row['description'],
                'price': Decimal(str(row['price'])),
                'stock': int(row['stock']),
                'image_url': row['image_url'],
                'seller_id': user_map.get(row['seller_username'], 'u1')
            }
            batch.put_item(Item=item)
            book_map[row['title']] = book_id
            print(f"  Synced: {row['title']}")
    return book_map

def process_orders(data_dir, user_map, book_map, use_s3):
    print("\n--- Syncing Orders ---")
    table = dynamodb.Table(TABLE_ORDERS)
    reader = get_csv_reader(os.path.join(data_dir, 'orders.csv'), "imports/orders.csv", use_s3)
    
    if not reader: return

    with table.batch_writer() as batch:
        for i, row in enumerate(reader, 1):
            item = {
                'id': f"o{i}",
                'user_id': user_map.get(row['buyer_username'], 'u1'),
                'book_id': book_map.get(row['book_title'], 'b1'),
                'quantity': int(row['quantity']),
                'total_price': Decimal(str(row['total_price'])),
                'status': row['status'],
                'order_date': row['order_date']
            }
            batch.put_item(Item=item)
            print(f"  Synced order for: {row['buyer_username']}")

if __name__ == "__main__":
    local_data = os.path.join(os.path.dirname(__file__), 'data')
    
    print("========================================")
    print("      ROBUST DYNAMODB SEEDER v2.0       ")
    print("========================================")
    
    s3_available = check_s3_availability()
    
    try:
        users = process_users(local_data, s3_available)
        books = process_books(local_data, users, s3_available)
        process_orders(local_data, users, books, s3_available)
        print("\nSUCCESS: All data successfully pushed to DynamoDB!")
    except Exception as e:
        print(f"\nFATAL ERROR DURING SYNC: {e}")
        exit(1)
