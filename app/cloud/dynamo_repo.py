import boto3
import os
from botocore.exceptions import ClientError
from decimal import Decimal

class DynamoBookRepository:
    """AWS DynamoDB implementation for Book repository."""
    
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
        self.table_name = os.environ.get('DYNAMODB_BOOKS_TABLE', 'BookBazaarBooks')
        self.table = self.dynamodb.Table(self.table_name)
        
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
