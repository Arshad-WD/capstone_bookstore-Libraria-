import boto3
import csv
import os
from decimal import Decimal
from werkzeug.security import generate_password_hash

# --- Configuration ---
AWS_REGION = "us-east-1"
TABLE_USERS = "BookBazaarUsers"
TABLE_BOOKS = "BookBazaarBooks"
TABLE_ORDERS = "BookBazaarOrders"

# --- Clients ---
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def get_decimal(val):
    try:
        if not val or val == '': return Decimal('0.0')
        return Decimal(str(val).replace(',', ''))
    except:
        return Decimal('0.0')

def seed_data(data_dir):
    print("========================================")
    print("   🚀 HIGH-PERFORMANCE CSV SEEDER 🚀   ")
    print("========================================")

    # 1. Sync Users
    users_file = os.path.join(data_dir, 'users.csv')
    user_map = {}
    if os.path.exists(users_file):
        print(f"\n--- Syncing Users from {os.path.basename(users_file)} ---")
        users_table = dynamodb.Table(TABLE_USERS)
        with open(users_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                user_id = f"u{i}"
                item = {
                    'id': user_id,
                    'username': row['username'],
                    'email': row['email'],
                    'role': row['role'],
                    'is_validated': row.get('is_validated', 'True').lower() == 'true',
                    'password_hash': generate_password_hash(row['password'])
                }
                users_table.put_item(Item=item)
                user_map[row['username']] = user_id
        print(f"  ✓ {len(user_map)} users synced.")
    else:
        print(f"  [SKIPPED] users.csv not found.")

    # 2. Sync Books (Use final if exists)
    books_file = os.path.join(data_dir, 'books_final.csv')
    if not os.path.exists(books_file):
        books_file = os.path.join(data_dir, 'books.csv')
    
    book_map = {}
    if os.path.exists(books_file):
        print(f"\n--- Syncing Books from {os.path.basename(books_file)} (Batching...) ---")
        books_table = dynamodb.Table(TABLE_BOOKS)
        
        with books_table.batch_writer() as batch:
            with open(books_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                count = 0
                for row in reader:
                    count += 1
                    book_id = f"b{count}"
                    
                    # Handle seller mapping
                    # If seller_username is missing (like in books_final), assign to first seller
                    seller_id = "u1" 
                    if 'seller_username' in row and row['seller_username'] in user_map:
                        seller_id = user_map[row['seller_username']]
                    elif len(user_map) > 0:
                        # Map to any seller found
                        seller_id = list(user_map.values())[0]

                    item = {
                        'id': book_id,
                        'title': row.get('title', 'Unknown'),
                        'author': row.get('author', 'Unknown'),
                        'description': row.get('description', ''),
                        'price': get_decimal(row.get('price', 0)),
                        'stock': int(row.get('stock', 50) or 50),
                        'seller_id': seller_id,
                        'image_url': row.get('image_url') or f"/static/images/placeholder.jpg"
                    }
                    batch.put_item(Item=item)
                    book_map[row.get('title')] = book_id
                    
                    if count % 1000 == 0:
                        print(f"  ... Sent {count} books")
        print(f"  ✓ {count} books synced via BatchWrite.")
    else:
        print(f"  [SKIPPED] book source not found.")

    # 3. Sync Orders
    orders_file = os.path.join(data_dir, 'orders.csv')
    if os.path.exists(orders_file):
        print(f"\n--- Syncing Orders from {os.path.basename(orders_file)} ---")
        orders_table = dynamodb.Table(TABLE_ORDERS)
        with orders_table.batch_writer() as batch:
            with open(orders_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                count = 0
                for row in reader:
                    count += 1
                    item = {
                        'id': f"o{count}",
                        'user_id': user_map.get(row['buyer_username'], 'u1'),
                        'book_id': book_map.get(row['book_title'], 'b1'),
                        'quantity': int(row.get('quantity', 1) or 1),
                        'total_price': get_decimal(row.get('total_price', 0)),
                        'status': row.get('status', 'Placed'),
                        'order_date': row.get('order_date', '2024-01-01 00:00:00')
                    }
                    batch.put_item(Item=item)
        print(f"  ✓ {count} orders synced.")
    else:
        print(f"  [SKIPPED] orders.csv not found.")

    print("\n========================================")
    print("   ✅ TOTAL DATA RELOADED SUCCESSFULLY   ")
    print("========================================")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(current_dir, 'data')
    
    try:
        seed_data(data_folder)
    except Exception as e:
        print(f"\n[ERROR] Bulk Seeding failed: {e}")
