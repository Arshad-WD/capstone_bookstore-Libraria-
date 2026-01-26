import boto3
import os
import sys

def setup_aws():
    """Create DynamoDB tables for BookBazaar."""
    print("Setting up AWS resources for BookBazaar...")
    
    region = os.environ.get('AWS_REGION', 'us-east-1')
    dynamodb = boto3.resource('dynamodb', region_name=region)
    
    # 1. Created Books Table
    try:
        print("Creating Books table...")
        table = dynamodb.create_table(
            TableName='BookBazaarBooks',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
        print("✓ Books table created.")
    except Exception as e:
        print(f"Error or table already exists: {e}")

    # 2. Create Orders Table
    try:
        print("Creating Orders table...")
        table = dynamodb.create_table(
            TableName='BookBazaarOrders',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.wait_until_exists()
        print("✓ Orders table created.")
    except Exception as e:
        print(f"Error or table already exists: {e}")

    # 3. Create SNS Topic
    try:
        print("Creating SNS Topic...")
        sns = boto3.client('sns', region_name=region)
        response = sns.create_topic(Name='BookBazaarNotifications')
        print(f"✓ SNS Topic created: {response['TopicArn']}")
        print(f"--- IMPORTANT: Add 'SNS_TOPIC_ARN={response['TopicArn']}' to your environment variables ---")
    except Exception as e:
        print(f"Error creating SNS topic: {e}")

    print("\nAWS environment setup complete.")

if __name__ == "__main__":
    # Check for AWS credentials
    try:
        boto3.client('sts').get_caller_identity()
    except Exception:
        print("ERROR: AWS credentials not found. Please run 'aws configure'.")
        sys.exit(1)
        
    setup_aws()
