import boto3
import os
import sys
import argparse
from botocore.exceptions import ClientError
from decimal import Decimal
from dotenv import load_dotenv

# Load environment variables for CLI usage
load_dotenv()

class AWSApp:
    """Central point for AWS resource management."""
    
    def __init__(self):
        self.region = os.environ.get('AWS_REGION', 'us-east-1')
        self._dynamodb = None
        self._sns = None
        
    @property
    def dynamodb(self):
        if self._dynamodb is None:
            self._dynamodb = boto3.resource('dynamodb', region_name=self.region)
        return self._dynamodb
        
    @property
    def sns(self):
        if self._sns is None:
            self._sns = boto3.client('sns', region_name=self.region)
        return self._sns

# Global instance for easy access
aws_app = AWSApp()

class SNSNotifier:
    """AWS SNS implementation for notifications."""
    
    def __init__(self, aws_instance=None):
        self.aws = aws_instance or aws_app
        self.topic_arn = os.environ.get('SNS_TOPIC_ARN')
        
    def send(self, email, message):
        """Publish message to SNS Topic."""
        if not self.topic_arn:
            print(f"[AWS SNS MOCK] No Topic ARN found. Notification for {email}: {message}")
            return
            
        try:
            self.aws.sns.publish(
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

class DynamoBookRepository:
    """AWS DynamoDB implementation for Book repository."""
    
    def __init__(self, aws_instance=None):
        self.aws = aws_instance or aws_app
        self.table_name = os.environ.get('DYNAMODB_BOOKS_TABLE', 'BookBazaarBooks')
        self.table = self.aws.dynamodb.Table(self.table_name)
        
    def get_all(self):
        """Scan table for all books."""
        try:
            response = self.table.scan()
            return response.get('Items', [])
        except ClientError as e:
            print(f"Error scanning DynamoDB: {e.response['Error']['Message']}")
            return []
            
    def get_by_id(self, book_id):
        """Get book by Partition Key."""
        try:
            response = self.table.get_item(Key={'id': str(book_id)})
            return response.get('Item')
        except ClientError as e:
            print(f"Error fetching from DynamoDB: {e.response['Error']['Message']}")
            return None
            
    def add(self, book_data):
        """Put item into DynamoDB."""
        try:
            # Convert float to Decimal for DynamoDB
            if 'price' in book_data:
                book_data['price'] = Decimal(str(book_data['price']))
                
            self.table.put_item(Item=book_data)
            return True
        except ClientError as e:
            print(f"Error adding to DynamoDB: {e.response['Error']['Message']}")
            return False

def setup_aws():
    """Setup AWS resources (DynamoDB tables and SNS topics)."""
    print("Setting up AWS resources for BookBazaar...")
    
    # 1. Create Books Table
    try:
        print("Creating Books table...")
        table = aws_app.dynamodb.create_table(
            TableName='BookBazaarBooks',
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        print("✓ Books table created.")
    except Exception as e:
        print(f"Books table: {e}")

    # 2. Create Orders Table
    try:
        print("Creating Orders table...")
        table = aws_app.dynamodb.create_table(
            TableName='BookBazaarOrders',
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        print("✓ Orders table created.")
    except Exception as e:
        print(f"Orders table: {e}")

    # 3. Create SNS Topic
    try:
        print("Creating SNS Topic...")
        response = aws_app.sns.create_topic(Name='BookBazaarNotifications')
        print(f"✓ SNS Topic created: {response['TopicArn']}")
        print(f"--- IMPORTANT: Add 'SNS_TOPIC_ARN={response['TopicArn']}' to your .env file ---")
    except Exception as e:
        print(f"SNS topic: {e}")

    print("\nAWS environment setup complete.")

def verify_aws():
    """Verify AWS connectivity and configuration."""
    print("Verifying AWS Integration")
    print("-" * 30)
    
    # Check credentials
    try:
        boto3.client('sts').get_caller_identity()
        print("[OK] AWS Credentials found.")
    except Exception:
        print("[FAIL] AWS credentials not found. Run 'aws configure'.")

    # 1. Test DynamoDB
    try:
        print("DynamoDB: ", end="", flush=True)
        repo = DynamoBookRepository()
        repo.table.scan(Limit=1)
        print("OK")
    except Exception as e:
        print(f"FAILED ({e})")

    # 2. Test SNS
    try:
        print("SNS: ", end="", flush=True)
        topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if not topic_arn:
            print("SKIPPED (No SNS_TOPIC_ARN in .env)")
        else:
            notifier = SNSNotifier()
            notifier.send("test@example.com", "Connectivity verification")
            print("OK")
    except Exception as e:
        print(f"FAILED ({e})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BookBazaar AWS Utility")
    parser.add_argument("command", choices=["setup", "verify"], help="Command to run")
    
    args = parser.parse_args()
    
    if args.command == "setup":
        setup_aws()
    elif args.command == "verify":
        verify_aws()
