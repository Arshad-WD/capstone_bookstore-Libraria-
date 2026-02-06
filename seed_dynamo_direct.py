import boto3
import csv
import os
from decimal import Decimal
from werkzeug.security import generate_password_hash

# --- Configuration (Update as needed) ---
AWS_REGION = "us-east-1"
TABLE_USERS = "BookBazaarUsers"
TABLE_BOOKS = "BookBazaarBooks"
TABLE_ORDERS = "BookBazaarOrders"

# --- Setup Boto3 ---
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def get_table(name):
    return dynamodb.Table(name)

def seed_users(data_dir):
    print(f"Seeding Users into {TABLE_USERS}...")
    table = get_table(TABLE_USERS)
    user_map = {}
    
    file_path = os.path.join(data_dir, 'users.csv')
    if not os.path.exists(file_path):
        print(f"  [SKIP] {file_path} not found.")
        return {}

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
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
            table.put_item(Item=item)
            user_map[row['username']] = user_id
            print(f"  Added: {row['username']}")
    return user_map

def seed_books(data_dir, user_map):
    print(f"Seeding Books into {TABLE_BOOKS}...")
    table = get_table(TABLE_BOOKS)
    book_map = {}
    
    file_path = os.path.join(data_dir, 'books.csv')
    if not os.path.exists(file_path):
        print(f"  [SKIP] {file_path} not found.")
        return {}

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
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
            table.put_item(Item=item)
            book_map[row['title']] = book_id
            print(f"  Added: {row['title']}")
    return book_map

def seed_orders(data_dir, user_map, book_map):
    print(f"Seeding Orders into {TABLE_ORDERS}...")
    table = get_table(TABLE_ORDERS)
    
    file_path = os.path.join(data_dir, 'orders.csv')
    if not os.path.exists(file_path):
        print(f"  [SKIP] {file_path} not found.")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
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
            table.put_item(Item=item)
            print(f"  Added order for: {row['buyer_username']}")

if __name__ == "__main__":
    # Path to your 'data' folder
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(current_dir, 'data')
    
    print("--- Starting Direct DynamoDB Seeding ---")
    
    try:
        users = seed_users(data_folder)
        books = seed_books(data_folder, users)
        seed_orders(data_folder, users, books)
        print("\nSUCCESS: All data successfully pushed to DynamoDB!")
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
