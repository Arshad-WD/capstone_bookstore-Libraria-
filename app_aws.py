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
        self._s3 = None
        self._iam = None

    def check_iam_permission(self, user_role, resource):
        """Simulate IAM policy check."""
        permissions = {
            'admin': ['*'],
            'seller': ['books:add', 'books:delete', 'sales:view'],
            'buyer': ['books:view', 'orders:create']
        }
        allowed = permissions.get(user_role, [])
        return '*' in allowed or resource in allowed
        
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
            
class DynamoUserRepository:
    """AWS DynamoDB implementation for User repository."""
    
    def __init__(self, aws_instance=None):
        self.aws = aws_instance or aws_app
        self.table_name = os.environ.get('DYNAMODB_USERS_TABLE', 'BookBazaarUsers')
        self.table = self.aws.dynamodb.Table(self.table_name)
        
    def get_by_email(self, email):
        """Get user by email (Global Secondary Index or scan)."""
        try:
            # Assuming email is a unique attribute, but not the primary key (id is PK)
            # For simplicity in this demo, we'll use scan. In production, use GSI.
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('email').eq(email)
            )
            items = response.get('Items', [])
            return items[0] if items else None
        except ClientError as e:
            print(f"Error fetching user from DynamoDB: {e.response['Error']['Message']}")
            return None
            
    def add(self, user_data):
        """Put user into DynamoDB."""
        try:
            self.table.put_item(Item=user_data)
            return True
        except ClientError as e:
            print(f"Error adding user to DynamoDB: {e.response['Error']['Message']}")
            return False

class DynamoOrderRepository:
    """AWS DynamoDB implementation for Order repository."""
    
    def __init__(self, aws_instance=None):
        self.aws = aws_instance or aws_app
        self.table_name = os.environ.get('DYNAMODB_ORDERS_TABLE', 'BookBazaarOrders')
        self.table = self.aws.dynamodb.Table(self.table_name)
        
    def add(self, order_data):
        """Put order into DynamoDB."""
        try:
            if 'total_price' in order_data:
                order_data['total_price'] = Decimal(str(order_data['total_price']))
            self.table.put_item(Item=order_data)
            return True
        except ClientError as e:
            print(f"Error adding order to DynamoDB: {e.response['Error']['Message']}")
            return False
            
    def get_by_seller_id(self, seller_id):
        """Scan for orders belonging to books owned by a seller."""
        # Note: In production, use GSI on seller_id for performance
        try:
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('seller_id').eq(seller_id)
            )
            return response.get('Items', [])
        except ClientError as e:
            print(f"Error fetching seller orders: {e.response['Error']['Message']}")
            return []

class S3Uploader:
    """AWS S3 implementation for file uploads."""
    
    def __init__(self, aws_instance=None):
        self.aws = aws_instance or aws_app
        self._s3_client = None
        self.bucket_name = os.environ.get('S3_BUCKET_NAME', 'bookbazaar-assets')
        
    @property
    def client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client('s3', region_name=self.aws.region)
        return self._s3_client
        
    def upload_file(self, file_path, object_name=None):
        """Upload a file to an S3 bucket."""
        if object_name is None:
            object_name = os.path.basename(file_path)
            
        try:
            self.client.upload_file(file_path, self.bucket_name, object_name)
            url = f"https://{self.bucket_name}.s3.{self.aws.region}.amazonaws.com/{object_name}"
            print(f"[AWS S3] File uploaded to {url}")
            return url
        except ClientError as e:
            print(f"[AWS S3 ERROR] {e}")
            return None

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

    # 4. Create Users Table
    try:
        print("Creating Users table...")
        table = aws_app.dynamodb.create_table(
            TableName='BookBazaarUsers',
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        print("✓ Users table created.")
    except Exception as e:
        print(f"Users table: {e}")

    # 5. Create S3 Bucket (Mocked for setup)
    try:
        print("Creating S3 Bucket...")
        s3 = boto3.client('s3', region_name=aws_app.region)
        s3.create_bucket(Bucket='bookbazaar-assets')
        print("✓ S3 Bucket created.")
    except Exception as e:
        print(f"S3 bucket: {e}")

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
