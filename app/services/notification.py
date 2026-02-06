import os
import app_aws

class LocalNotifier:
    def send(self, email, message):
        print(f"[LOCAL NOTIFICATION] {email}: {message}")

class NotificationService:
    def __init__(self):
        # Prefer environment variable, fallback to hardcoded value in app_aws.py
        self.sns_topic_arn = os.environ.get('SNS_TOPIC_ARN') or app_aws.SNS_TOPIC_ARN
        
        # Check if topic is set (not placeholder)
        if self.sns_topic_arn and "123456789012" not in self.sns_topic_arn:
            self.notifier = app_aws.SNSNotifier()
        else:
            self.notifier = LocalNotifier()

    def send(self, email, message):
        self.notifier.send(email, message)