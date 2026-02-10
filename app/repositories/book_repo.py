import json
import base64
from app.extensions import db
from app.models.book import Book
from app_aws import DynamoBookRepository

class BookRepository:
    def get_all_paginated(self, page, per_page, token=None):
        """Get books from DynamoDB using token-based pagination."""
        try:
            dynamo = DynamoBookRepository()
            
            # Decode token if present
            last_key = None
            if token:
                try:
                    last_key = json.loads(base64.b64decode(token).decode('utf-8'))
                except:
                    pass

            response = dynamo.get_paginated(limit=per_page, last_key=last_key)
            items = response['Items']
            
            # If no items found in DynamoDB on first page, check if we should fallback to SQL
            if not items and not token:
                print("No books found in DynamoDB first page, falling back to SQL...")
                return Book.query.order_by(Book.id.desc()).paginate(page=page, per_page=per_page, error_out=False)

            next_key = response['LastEvaluatedKey']
            
            # Encode next token
            next_token = None
            if next_key:
                next_token = base64.b64encode(json.dumps(next_key).encode('utf-8')).decode('utf-8')

            books = []
            for item in items:
                book = Book(
                    title=item.get('title'),
                    author=item.get('author'),
                    description=item.get('description'),
                    price=float(item.get('price', 0)),
                    stock=int(item.get('stock', 0)),
                    image_url=item.get('image_url')
                )
                book.id = item.get('id')
                books.append(book)
            
            # Mocking the pagination object structure expected by templates
            class MockPagination:
                def __init__(self, items, page, per_page, next_token):
                    self.items = items
                    self.page = page
                    self.per_page = per_page
                    self.next_token = next_token
                    self.has_next = next_token is not None
                    # We don't have total count anymore for infinite datasets
                    self.total = 99999 
                    self.pages = 9999 
                def iter_pages(self, **kwargs):
                    return [] # Simplify for token-based

            return MockPagination(books, page, per_page, next_token)
        except Exception as e:
            print(f"DynamoDB Read Error: {e}")
            return Book.query.order_by(Book.id.desc()).paginate(page=page, per_page=per_page, error_out=False)
    
    def search_paginated(self, query, page, per_page):
        """Search books (Fallback to get_all for simplicity on Dynamo)."""
        return self.get_all_paginated(page, per_page)

    def get_by_id(self, book_id):
        """Get a book by ID from DynamoDB."""
        try:
            dynamo = DynamoBookRepository()
            item = dynamo.get_by_id(str(book_id))
            if item:
                book = Book(
                    title=item.get('title'),
                    author=item.get('author'),
                    description=item.get('description'),
                    price=float(item.get('price', 0)),
                    stock=int(item.get('stock', 0)),
                    image_url=item.get('image_url')
                )
                book.id = item.get('id')
                return book
        except Exception as e:
            print(f"DynamoDB Read Error: {e}")
            
        return Book.query.get(book_id)
    
    def add(self, book):
        """Add a new book to database and DynamoDB."""
        db.session.add(book)
        db.session.commit()
        
        # Sync to DynamoDB
        try:
            dynamo = DynamoBookRepository()
            dynamo.add({
                'id': str(book.id),
                'title': book.title,
                'author': book.author,
                'price': book.price,
                'stock': book.stock,
                'seller_id': str(book.seller_id) if book.seller_id else "system",
                'image_url': book.image_url or f"/static/images/placeholder.jpg"
            })
        except Exception as e:
            print(f"DynamoDB Sync Error: {e}")
            
        return book
    
    def update(self, book):
        """Update an existing book."""
        db.session.commit()
        return book
    