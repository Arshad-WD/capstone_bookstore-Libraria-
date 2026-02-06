from app.extensions import db
from app.models.book import Book
from app_aws import DynamoBookRepository

class BookRepository:
    def get_all_paginated(self, page, per_page):
        """Get books from DynamoDB."""
        try:
            dynamo = DynamoBookRepository()
            items = dynamo.get_all()
            
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
            
            # Simple manual pagination for the list
            start = (page - 1) * per_page
            end = start + per_page
            
            # Mocking the pagination object structure expected by templates
            class MockPagination:
                def __init__(self, items, page, per_page, total):
                    self.items = items
                    self.page = page
                    self.per_page = per_page
                    self.total = total
                    self.pages = (total // per_page) + (1 if total % per_page > 0 else 0)
                    self.has_prev = page > 1
                    self.has_next = page < self.pages
                    self.prev_num = page - 1
                    self.next_num = page + 1
                def iter_pages(self):
                    return range(1, self.pages + 1)

            return MockPagination(books[start:end], page, per_page, len(books))
        except Exception as e:
            print(f"DynamoDB Read Error: {e}")
            return Book.query.order_by(Book.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    
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
                'image_url': book.image_url or ""
            })
        except Exception as e:
            print(f"DynamoDB Sync Error: {e}")
            
        return book
    
    def update(self, book):
        """Update an existing book."""
        db.session.commit()
        return book
    