import boto3
import os
from botocore.exceptions import ClientError

class SNSNotifier:
    """AWS SNS implementation for notifications."""
    
    def __init__(self):
        self.sns = boto3.client('sns', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        self.topic_arn = os.environ.get('SNS_TOPIC_ARN')
        
    def send(self, email, message):
        """Publish message to SNS Topic."""
        if not self.topic_arn:
            print(f"[AWS SNS MOCK] No Topic ARN found. Notification for {email}: {message}")
            return
            
        try:
            self.sns.publish(
                TopicArn=self.topic_arn,
                Message=message,
                Subject="BookBazaar Order Update",
                MessageAttributes={
                    'email': {
                        'DataType': 'String',
                        'StringValue': email
                    }
                }
            )
            print(f"[AWS SNS] Notification sent to {email}")
        except ClientError as e:
            print(f"[AWS SNS ERROR] {e.response['Error']['Message']}")
