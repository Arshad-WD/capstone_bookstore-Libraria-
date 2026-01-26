from app.extensions import db
from app.models.book import Book

class BookRepository:
    def get_all(self):
        """Get all books from database."""
        return Book.query.all()
    
    def get_by_id(self, book_id):
        """Get a book by ID."""
        return Book.query.get(book_id)
    
    def add(self, book):
        """Add a new book to database."""
        db.session.add(book)
        db.session.commit()
        return book
    
    def update(self, book):
        """Update an existing book."""
        db.session.commit()
        return book
    