import boto3
import os
import sys
from decimal import Decimal
from botocore.exceptions import ClientError

# Add current directory to path so we can import app
sys.path.append(os.getcwd())

from app.cloud.dynamo_repo import DynamoBookRepository
from app.cloud.aws_sns import SNSNotifier

def verify_dynamodb():
    print("\n--- Verifying DynamoDB ---")
    try:
        repo = DynamoBookRepository()
        test_book = {
            'id': 'test-999',
            'title': 'AWS Verification Test Book',
            'author': 'Test Bot',
            'price': 219.99,
            'stock': 10
        }
        
        print(f"Adding test book {test_book['id']}...")
        if repo.add(test_book):
            print("✓ Successfully added book.")
        else:
            print("✗ Failed to add book.")
            return False
            
        print("Retrieving test book...")
        retrieved = repo.get_by_id('test-999')
        if retrieved and retrieved['title'] == test_book['title']:
            print(f"✓ Successfully retrieved: {retrieved['title']}")
        else:
            print("✗ Failed to retrieve or data mismatch.")
            return False
            
        return True
    except Exception as e:
        print(f"✗ DynamoDB Verification Error: {e}")
        return False

def verify_sns():
    print("\n--- Verifying SNS ---")
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if not topic_arn:
        print("! Skipping SNS test: SNS_TOPIC_ARN environment variable not set.")
        return True
        
    try:
        notifier = SNSNotifier()
        print(f"Sending test notification to {topic_arn}...")
        notifier.send("test@example.com", "This is a test notification from Libraria verification script.")
        print("✓ SNS test command issued (check your inbox/subscription if applicable).")
        return True
    except Exception as e:
        print(f"✗ SNS Verification Error: {e}")
        return False

if __name__ == "__main__":
    print("Libraria AWS Integration Verification")
    print("=====================================")
    
    # Simple check for AWS Region
    region = os.environ.get('AWS_REGION', 'us-east-1')
    print(f"Using AWS Region: {region}")
    
    db_ok = verify_dynamodb()
    sns_ok = verify_sns()
    
    print("\n=====================================")
    if db_ok and sns_ok:
        print("OVERALL STATUS: SUCCESS")
    else:
        print("OVERALL STATUS: FAILED")
