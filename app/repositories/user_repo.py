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
        """Fetch user by email from DynamoDB first, then fallback to SQL."""
        try:
            dynamo = DynamoUserRepository()
            item = dynamo.get_by_email(email)
            if item:
                # Map DynamoDB item back to User object for the app
                user = User(
                    username=item.get('username'),
                    email=item.get('email'),
                    role=item.get('role'),
                    is_validated=item.get('is_validated', False)
                )
                user.id = item.get('id')
                user.password_hash = item.get('password_hash')
                return user
        except Exception as e:
            print(f"DynamoDB Read Error: {e}")
            
        return User.query.filter_by(email=email).first()