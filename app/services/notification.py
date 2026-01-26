
class LocalNotifier:
    def send(self, email, message):
        print(f"[NOTIFICATION] {email}: {message}")