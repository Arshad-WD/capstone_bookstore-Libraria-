from app.extensions import db
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default="buyer")  # Roles: buyer, seller, admin
    is_validated = db.Column(db.Boolean, default=False)  # For seller verification
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password):
        password = password.strip()
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        password = password.strip()
        return check_password_hash(self.password_hash, password)