from app.extensions import db
from app.models.user import User

class UserRepository:
    def create(self, user):
        db.session.add(user)
        db.session.commit()
        
    def get_by_email(self, email):
        return User.query.filter_by(email=email).first()