import os
from app.cloud.aws_sns import SNSNotifier

class LocalNotifier:
    def send(self, email, message):
        print(f"[LOCAL NOTIFICATION] {email}: {message}")

class NotificationService:
    def __init__(self):
        self.sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if self.sns_topic_arn:
            self.notifier = SNSNotifier()
        else:
            self.notifier = LocalNotifier()

    def send(self, email, message):
        self.notifier.send(email, message)