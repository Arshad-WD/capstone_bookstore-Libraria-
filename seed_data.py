import csv
import os
from datetime import datetime
from app import create_app
from app.extensions import db
from app.models.user import User
from app.models.book import Book
from app.models.order import Order

def seed_users(csv_file):
    print(f"Seeding users from {csv_file}...")
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if User.query.filter_by(username=row['username']).first():
                print(f"User {row['username']} already exists, skipping.")
                continue
            
            user = User(
                username=row['username'],
                email=row['email'],
                role=row['role'],
                is_validated=row['is_validated'].lower() == 'true'
            )
            user.set_password(row['password'])
            db.session.add(user)
    db.session.commit()
    print("✓ Users seeded.")

def seed_books(csv_file):
    print(f"Seeding books from {csv_file}...")
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if Book.query.filter_by(title=row['title']).first():
                print(f"Book '{row['title']}' already exists, skipping.")
                continue
            
            seller = User.query.filter_by(username=row['seller_username']).first()
            if not seller:
                print(f"Warning: Seller {row['seller_username']} not found for book {row['title']}. Skipping.")
                continue
                
            book = Book(
                title=row['title'],
                author=row['author'],
                description=row['description'],
                price=float(row['price']),
                stock=int(row['stock']),
                image_url=row['image_url'] if row['image_url'] else None,
                seller_id=seller.id
            )
            db.session.add(book)
    db.session.commit()
    print("✓ Books seeded.")

def seed_orders(csv_file):
    print(f"Seeding orders from {csv_file}...")
    with open(csv_file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            buyer = User.query.filter_by(username=row['buyer_username']).first()
            book = Book.query.filter_by(title=row['book_title']).first()
            
            if not buyer or not book:
                print(f"Warning: Buyer or Book not found for order. Skipping.")
                continue
                
            order = Order(
                user_id=buyer.id,
                book_id=book.id,
                quantity=int(row['quantity']),
                total_price=float(row['total_price']),
                status=row['status'],
                order_date=datetime.strptime(row['order_date'], '%Y-%m-%d %H:%M:%S')
            )
            db.session.add(order)
    db.session.commit()
    print("✓ Orders seeded.")

def run_seeder():
    app = create_app()
    with app.app_context():
        base_path = r"d:\Projects Unknown\capstone_bookstore-Libraria-\data"
        
        seed_users(os.path.join(base_path, "users.csv"))
        seed_books(os.path.join(base_path, "books.csv"))
        seed_orders(os.path.join(base_path, "orders.csv"))
        
        print("\nAll data seeded successfully! 🚀")

if __name__ == "__main__":
    run_seeder()
