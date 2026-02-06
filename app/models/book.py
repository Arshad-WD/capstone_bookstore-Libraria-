from app.extensions import db
from datetime import datetime

class Book(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(150), nullable=False)
    author = db.Column(db. String(200), nullable=False)
    description = db.Column(db.Text)
    price = db.Column(db.Float, nullable=False)
    stock = db.Column(db.Integer, default=0)
    seller_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=True)  # Books can be owned by sellers
    image_url = db.Column(db.String(500))
    
    # Relationships
    seller = db.relationship('User', backref=db.backref('books', lazy=True))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)