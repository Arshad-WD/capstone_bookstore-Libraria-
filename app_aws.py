import boto3
import os
import sys
import argparse
from botocore.exceptions import ClientError
from decimal import Decimal
from werkzeug.security import generate_password_hash
# Hardcoded Configuration (Edit these directly)
AWS_REGION = "us-east-1"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:861276080904:BookBazaarNotifications"

# DynamoDB Table Names
DYNAMODB_BOOKS_TABLE = "BookBazaarBooks"
DYNAMODB_USERS_TABLE = "BookBazaarUsers"
DYNAMODB_ORDERS_TABLE = "BookBazaarOrders"

class AWSApp:
    """Central point for AWS resource management."""
    
    def __init__(self):
        self.region = AWS_REGION
        self._dynamodb = None
        self._sns = None
        self._sns = None
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
        self.topic_arn = SNS_TOPIC_ARN
        
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
        self.table_name = DYNAMODB_BOOKS_TABLE
        self.table = self.aws.dynamodb.Table(self.table_name)
        
    def get_paginated(self, limit=8, last_key=None):
        """Query Books table using TypeIndex for efficient pagination."""
        try:
            query_params = {
                'IndexName': 'TypeIndex',
                'KeyConditionExpression': boto3.dynamodb.conditions.Key('type').eq('book'),
                'Limit': limit
            }
            if last_key:
                query_params['ExclusiveStartKey'] = last_key
            
            response = self.table.query(**query_params)
            return {
                'Items': response.get('Items', []),
                'LastEvaluatedKey': response.get('LastEvaluatedKey')
            }
        except ClientError as e:
            print(f"Error querying DynamoDB: {e.response['Error']['Message']}")
            return {'Items': [], 'LastEvaluatedKey': None}

    def add(self, book_data):
        """Put item into DynamoDB."""
        try:
            # Add type for GSI grouping
            book_data['type'] = 'book'
            
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
        self.table_name = DYNAMODB_USERS_TABLE
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
        self.table_name = DYNAMODB_ORDERS_TABLE
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


def setup_aws():
    """Setup AWS resources (DynamoDB tables and SNS topics)."""
    print("Setting up AWS resources for BookBazaar...")
    
    # 1. Create Books Table
    try:
        print("Creating Books table with TypeIndex GSI...")
        table = aws_app.dynamodb.create_table(
            TableName='BookBazaarBooks',
            KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'id', 'AttributeType': 'S'},
                {'AttributeName': 'type', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'TypeIndex',
                    'KeySchema': [
                        {'AttributeName': 'type', 'KeyType': 'HASH'},
                        {'AttributeName': 'id', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.wait_until_exists()
        print("✓ Books table created with indexing.")
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

    try:
        print("SNS: ", end="", flush=True)
        topic_arn = SNS_TOPIC_ARN
        if not topic_arn or "123456789012" in topic_arn:
            print("SKIPPED (Update SNS_TOPIC_ARN at the top of app_aws.py)")
        else:
            notifier = SNSNotifier()
            notifier.send("test@example.com", "Connectivity verification")
            print("OK")
    except Exception as e:
        print(f"FAILED ({e})")

def seed_db():
    """Seed DynamoDB tables from CSV files."""
    import csv
    import os
    
    # Path setup
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_path, 'data')
    
    if not os.path.exists(data_dir):
        print(f"[ERROR] Data directory not found at {data_dir}")
        return

    print(f"Seeding data from {data_dir}...")
    user_repo = DynamoUserRepository()
    book_repo = DynamoBookRepository()
    order_repo = DynamoOrderRepository()

    # 1. Seed Users
    user_map = {} # username -> id mapping for relationships
    try:
        with open(os.path.join(data_dir, 'users.csv'), 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                user_id = f"u{i}"
                user_data = {
                    'id': user_id,
                    'username': row['username'],
                    'email': row['email'],
                    'role': row['role'],
                    'is_validated': row['is_validated'].lower() == 'true',
                    'password_hash': generate_password_hash(row['password'])
                }
                user_repo.add(user_data)
                user_map[row['username']] = user_id
                print(f"  Added user: {row['username']}")
        print("✓ Users seeded.")
    except Exception as e:
        print(f"Error seeding users: {e}")

    # 2. Seed Books
    book_map = {} # title -> id
    try:
        with open(os.path.join(data_dir, 'books.csv'), 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                book_id = f"b{i}"
                seller_id = user_map.get(row['seller_username'], 'u1')
                book_data = {
                    'id': book_id,
                    'title': row['title'],
                    'author': row['author'],
                    'description': row['description'],
                    'price': float(row['price']),
                    'stock': int(row['stock']),
                    'image_url': row['image_url'],
                    'seller_id': seller_id
                }
                book_repo.add(book_data)
                book_map[row['title']] = book_id
        print("✓ Books seeded.")
    except Exception as e:
        print(f"Error seeding books: {e}")

    # 3. Seed Orders
    try:
        with open(os.path.join(data_dir, 'orders.csv'), 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader, 1):
                order_data = {
                    'id': f"o{i}",
                    'user_id': user_map.get(row['buyer_username'], 'u1'),
                    'book_id': book_map.get(row['book_title'], 'b1'),
                    'quantity': int(row['quantity']),
                    'total_price': float(row['total_price']),
                    'status': row['status'],
                    'order_date': row['order_date']
                }
                order_repo.add(order_data)
        print("✓ Orders seeded.")
    except Exception as e:
        print(f"Error seeding orders: {e}")
    
    print("\n✓ DynamoDB Seeding complete!")

def run_server():
    """Start the Flask web server."""
    print("Starting BookBazaar Web Server on AWS...")
    print("Accessible at: http://YOUR-EC2-PUBLIC-IP:5000")
    
    # Ensure current directory is in path
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    try:
        from app import create_app
        app = create_app()
        # Ensure it listens on 0.0.0.0 for EC2 access
        app.run(host='0.0.0.0', port=5000, debug=False)
    except ImportError as e:
        print(f"\n[ERROR] Module import failed: {e}")
        print("This usually means a dependency (like Flask) is not installed.")
        print("Try running: pip3 install -r requirements.txt")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n[ERROR] Failed to start server: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BookBazaar AWS Utility")
    parser.add_argument("command", choices=["setup", "verify", "run", "seed"], 
                        nargs='?', default="run",
                        help="Command to run (setup, verify, run, seed). Default is 'run'.")
    
    args = parser.parse_args()
    
    if args.command == "setup":
        setup_aws()
    elif args.command == "verify":
        verify_aws()
    elif args.command == "run":
        run_server()
    elif args.command == "seed":
        seed_db()
