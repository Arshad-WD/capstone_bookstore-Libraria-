from app.extensions import db
from app.models.book import Book
from app_aws import DynamoBookRepository

class BookRepository:
    def get_all_paginated(self, page, per_page):
        """Get paginated books from database."""
        return Book.query.order_by(Book.created_at.desc()).paginate(page=page, per_page=per_page, error_out=False)
    
    def search_paginated(self, query, page, per_page):
        """Search books with pagination."""
        if not query:
            return self.get_all_paginated(page, per_page)
        return Book.query.filter(
            (Book.title.ilike(f"%{query}%")) | 
            (Book.author.ilike(f"%{query}%"))
        ).order_by(Book.id.desc()).paginate(page=page, per_page=per_page, error_out=False)

    def get_by_id(self, book_id):
        """Get a book by ID."""
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
    