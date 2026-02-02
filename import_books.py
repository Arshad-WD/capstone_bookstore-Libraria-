import csv
import os
from app import create_app
from app.extensions import db
from app.models.book import Book

def import_books(csv_file_path):
    app = create_app()
    with app.app_context():
        print(f"Starting import from {csv_file_path}...")
        
        try:
            with open(csv_file_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                count = 0
                for row in reader:
                    # Basic cleaning
                    title = row.get('title', 'Unknown Title').strip()
                    author = row.get('author', 'Unknown Author').strip()
                    description = row.get('description', '').strip()
                    
                    try:
                        price = float(row.get('price', 0))
                    except (ValueError, TypeError):
                        price = 0.0
                        
                    image_url = row.get('image_url', '').strip()
                    
                    # Create book object
                    book = Book(
                        title=title,
                        author=author,
                        description=description,
                        price=price,
                        stock=50,  # Default stock
                        image_url=image_url if image_url else None
                    )
                    
                    db.session.add(book)
                    count += 1
                    
                    # Commit in batches for performance
                    if count % 100 == 0:
                        db.session.commit()
                        print(f"Imported {count} books...")
                
                db.session.commit()
                print(f"Finished! Total books imported: {count}")
                
        except FileNotFoundError:
            print(f"Error: File not found at {csv_file_path}")
        except Exception as e:
            print(f"An error occurred: {e}")
            db.session.rollback()

if __name__ == "__main__":
    # Use absolute path for reliability
    data_path = r"d:\Projects Unknown\capstone_bookstore-Libraria-\data\books_final.csv"
    import_books(data_path)
