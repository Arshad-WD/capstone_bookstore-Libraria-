from app.models.user import User
from app.repositories.user_repo import UserRepository

repo = UserRepository()

class AuthService:
    def register(self, data):
        user = User(
            username=data["username"].strip(),
            email=data["email"].strip()
        )
        user.set_password(data["password"])
        repo.create(user)

    def login(self, email, password):
        user = repo.get_by_email(email)
        if user and user.check_password(password):
            return user
        return None