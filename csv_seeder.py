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

def seed_from_csv(data_dir):
    print("========================================")
    print("      LOCAL CSV TO DYNAMODB SEEDER     ")
    print("========================================")

    # 1. Sync Users
    print("\n--- Processing Users (users.csv) ---")
    users_table = dynamodb.Table(TABLE_USERS)
    user_map = {} # username -> id
    
    users_file = os.path.join(data_dir, 'users.csv')
    if not os.path.exists(users_file):
        print(f"  [ERROR] {users_file} not found.")
        return

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
            print(f"  ✓ User added: {row['username']}")

    # 2. Sync Books
    print("\n--- Processing Books (books.csv) ---")
    books_table = dynamodb.Table(TABLE_BOOKS)
    book_map = {} # title -> id
    
    books_file = os.path.join(data_dir, 'books.csv')
    if not os.path.exists(books_file):
        print(f"  [ERROR] {books_file} not found.")
    else:
        with open(books_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                book_id = f"b{i}"
                item = {
                    'id': book_id,
                    'title': row['title'],
                    'author': row['author'],
                    'description': row['description'],
                    'price': Decimal(str(row['price'] or 0)),
                    'stock': int(row['stock'] or 0),
                    'seller_id': user_map.get(row['seller_username'], 'u1'),
                    'image_url': row['image_url'] or f"/static/images/placeholder.jpg"
                }
                books_table.put_item(Item=item)
                book_map[row['title']] = book_id
                print(f"  ✓ Book added: {row['title']}")

    # 3. Sync Orders
    print("\n--- Processing Orders (orders.csv) ---")
    orders_table = dynamodb.Table(TABLE_ORDERS)
    
    orders_file = os.path.join(data_dir, 'orders.csv')
    if not os.path.exists(orders_file):
        print(f"  [ERROR] {orders_file} not found.")
    else:
        with open(orders_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                item = {
                    'id': f"o{i}",
                    'user_id': user_map.get(row['buyer_username'], 'u1'),
                    'book_id': book_map.get(row['book_title'], 'b1'),
                    'quantity': int(row.get('quantity', 1)),
                    'total_price': Decimal(str(row.get('total_price', 0))),
                    'status': row.get('status', 'Placed'),
                    'order_date': row.get('order_date', '2024-01-01 00:00:00')
                }
                orders_table.put_item(Item=item)
                print(f"  ✓ Order synced for: {row['buyer_username']}")

    print("\n========================================")
    print("   SUCCESS: YOUR DATA IS NOW IN CLOUD   ")
    print("========================================")

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_folder = os.path.join(current_dir, 'data')
    
    try:
        seed_from_csv(data_folder)
    except Exception as e:
        print(f"\n[ERROR] Seeding failed: {e}")
