import boto3
from decimal import Decimal
from werkzeug.security import generate_password_hash

# --- Configuration ---
AWS_REGION = "us-east-1"
TABLE_USERS = "BookBazaarUsers"
TABLE_BOOKS = "BookBazaarBooks"
TABLE_ORDERS = "BookBazaarOrders"

# --- Clients ---
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)

def seed_official_data():
    print("========================================")
    print("      OFFICIAL DATA SEEDER (DYNAMO)     ")
    print("========================================")

    # 1. Users
    print("\nSyncing Official Users...")
    users_table = dynamodb.Table(TABLE_USERS)
    official_users = [
        {
            'id': 'u1',
            'username': 'demo_user',
            'email': 'demo@bookbazaar.com',
            'password': 'demo123',
            'role': 'customer',
            'is_validated': True
        },
        {
            'id': 'u2',
            'username': 'alice_buyer',
            'email': 'alice@buyer.com',
            'password': 'buyer123',
            'role': 'customer',
            'is_validated': True
        },
        {
            'id': 'u3',
            'username': 'bob_seller',
            'email': 'bob@seller.com',
            'password': 'seller123',
            'role': 'seller',
            'is_validated': True
        },
        {
            'id': 'u4',
            'username': 'admin_root',
            'email': 'admin@bookbazaar.com',
            'password': 'admin123',
            'role': 'admin',
            'is_validated': True
        }
    ]

    for u in official_users:
        item = {
            'id': u['id'],
            'username': u['username'],
            'email': u['email'],
            'role': u['role'],
            'is_validated': u['is_validated'],
            'password_hash': generate_password_hash(u['password'])
        }
        users_table.put_item(Item=item)
        print(f"  ✓ User added: {u['username']} ({u['email']})")

    # 2. Books
    print("\nSyncing Official Books...")
    books_table = dynamodb.Table(TABLE_BOOKS)
    official_books = [
        {
            'id': 'b1',
            'title': 'The Great Gatsby',
            'author': 'F. Scott Fitzgerald',
            'price': 15.99,
            'stock': 100,
            'seller_id': 'u3',
            'image_url': '/static/images/gatsby.jpg'
        },
        {
            'id': 'b2',
            'title': '1984',
            'author': 'George Orwell',
            'price': 12.50,
            'stock': 50,
            'seller_id': 'u3',
            'image_url': '/static/images/1984.jpg'
        },
        {
            'id': 'b3',
            'title': 'The Hobbit',
            'author': 'J.R.R. Tolkien',
            'price': 25.00,
            'stock': 30,
            'seller_id': 'u3',
            'image_url': '/static/images/hobbit.jpg'
        },
        {
            'id': 'b4',
            'title': 'Clean Code',
            'author': 'Robert C. Martin',
            'price': 45.00,
            'stock': 20,
            'seller_id': 'u3',
            'image_url': '/static/images/cleancode.jpg'
        },
        {
            'id': 'b5',
            'title': 'Atomic Habits',
            'author': 'James Clear',
            'price': 18.20,
            'stock': 80,
            'seller_id': 'u3',
            'image_url': '/static/images/atomic.jpg'
        }
    ]

    for b in official_books:
        item = {
            'id': b['id'],
            'title': b['title'],
            'author': b['author'],
            'price': Decimal(str(b['price'])),
            'stock': b['stock'],
            'seller_id': b['seller_id'],
            'image_url': b['image_url']
        }
        books_table.put_item(Item=item)
        print(f"  ✓ Book added: {b['title']}")

    print("\n========================================")
    print("   SUCCESS: DEMO DATA IS NOW IN CLOUD   ")
    print("========================================")

if __name__ == "__main__":
    try:
        seed_official_data()
    except Exception as e:
        print(f"\n[ERROR] Seeding failed: {e}")
