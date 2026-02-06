import boto3
import csv
import io
import os
from decimal import Decimal
from werkzeug.security import generate_password_hash
from botocore.exceptions import ClientError

# --- Configuration (Sync with app_aws.py) ---
AWS_REGION = "us-east-1"
S3_BUCKET = "bookbazaar-assets"
TABLE_USERS = "BookBazaarUsers"
TABLE_BOOKS = "BookBazaarBooks"
TABLE_ORDERS = "BookBazaarOrders"

# --- Clients ---
s3_client = boto3.client('s3', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def upload_to_s3(local_path, s3_key):
    """Upload local file to S3, creating bucket if necessary."""
    print(f"Uploading {local_path} to s3://{S3_BUCKET}/{s3_key}...")
    try:
        # Try to create bucket if it doesn't exist
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET)
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchBucket':
                print(f"  Bucket {S3_BUCKET} not found. Creating...")
                if AWS_REGION == 'us-east-1':
                    s3_client.create_bucket(Bucket=S3_BUCKET)
                else:
                    s3_client.create_bucket(
                        Bucket=S3_BUCKET,
                        CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
                    )
                print(f"  ✓ Bucket created.")
            else:
                raise e

        s3_client.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"  ✓ Uploaded.")
        return True
    except Exception as e:
        print(f"  [ERROR] Upload failed: {e}")
        return False

def get_csv_from_s3(s3_key):
    """Read CSV content directly from S3."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return csv.DictReader(io.StringIO(content))
    except Exception as e:
        print(f"  [ERROR] Failed to read from S3: {e}")
        return None

def process_users(s3_key):
    print(f"Processing Users from S3 ({s3_key})...")
    table = dynamodb.Table(TABLE_USERS)
    reader = get_csv_from_s3(s3_key)
    user_map = {}
    
    if not reader: return {}

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
            print(f"  Prepared: {row['username']}")
    print("✓ Users synced to DynamoDB.")
    return user_map

def process_books(s3_key, user_map):
    print(f"Processing Books from S3 ({s3_key})...")
    table = dynamodb.Table(TABLE_BOOKS)
    reader = get_csv_from_s3(s3_key)
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
            print(f"  Prepared: {row['title']}")
    print("✓ Books synced to DynamoDB.")
    return book_map

def process_orders(s3_key, user_map, book_map):
    print(f"Processing Orders from S3 ({s3_key})...")
    table = dynamodb.Table(TABLE_ORDERS)
    reader = get_csv_from_s3(s3_key)
    
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
            print(f"  Prepared order for: {row['buyer_username']}")
    print("✓ Orders synced to DynamoDB.")

if __name__ == "__main__":
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    print("=== S3 to DynamoDB Data Pipeline ===")
    
    # 1. Upload
    files = ['users.csv', 'books.csv', 'orders.csv']
    all_uploaded = True
    for f in files:
        if not upload_to_s3(os.path.join(data_dir, f), f"imports/{f}"):
            all_uploaded = False
    
    if not all_uploaded:
        print("\n[STOP] Pipeline halted: S3 Uploads failed.")
        exit(1)

    # 2. Sync
    try:
        users = process_users("imports/users.csv")
        if not users: raise Exception("User sync failed or S3 file empty")
        
        books = process_books("imports/books.csv", users)
        if not books: raise Exception("Book sync failed or S3 file empty")

        process_orders("imports/orders.csv", users, books)
        print("\nPIPELINE SUCCESS: Data moved through S3 to DynamoDB!")
    except Exception as e:
        print(f"\nPIPELINE ERROR: {e}")
        exit(1)
