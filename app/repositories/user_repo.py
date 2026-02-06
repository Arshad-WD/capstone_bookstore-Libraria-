from app.extensions import db
from app.models.user import User
from app_aws import DynamoUserRepository

class UserRepository:
    def create(self, user):
        """Create a new user in SQL and DynamoDB."""
        db.session.add(user)
        db.session.commit()
        
        # Sync to DynamoDB
        try:
            dynamo = DynamoUserRepository()
            dynamo.add({
                'id': str(user.id),
                'username': user.username,
                'email': user.email,
                'role': user.role,
                'password_hash': user.password_hash  # Consistent with cloud user mgmt
            })
        except Exception as e:
            print(f"DynamoDB Sync Error: {e}")
        
    def get_by_email(self, email):
        return User.query.filter_by(email=email).first()