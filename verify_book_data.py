
import sys
import os
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add current directory to path
sys.path.append(os.getcwd())

from app_aws import DYNAMODB_BOOKS_TABLE, AWS_REGION

def verify_books():
    print(f"Verifying Books Table: {DYNAMODB_BOOKS_TABLE} in {AWS_REGION}")
    
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamodb.Table(DYNAMODB_BOOKS_TABLE)
    
    # 1. Check Table Status and GSI Status
    try:
        print(f"Table Status: {table.table_status}")
        if table.global_secondary_indexes:
            for index in table.global_secondary_indexes:
                print(f"  - GSI: {index['IndexName']}, Status: {index['IndexStatus']}")
        else:
            print("  [WARNING] No Global Secondary Indexes found on table!")
    except Exception as e:
        print(f"Error describing table: {e}")
        return

    # 2. Check Raw Data (Scan)
    print("\n--- Raw Data Scan (Limit 5) ---")
    try:
        response = table.scan(Limit=5)
        items = response.get('Items', [])
        print(f"Found {len(items)} items via Scan.")
        for item in items:
            b_id = item.get('id', 'UNKNOWN')
            title = item.get('title', 'UNKNOWN')
            type_attr = item.get('type', '[MISSING]')
            print(f"  - ID: {b_id}, Title: {title[:20]}, Type: {type_attr}")
            
            if type_attr == '[MISSING]':
                print("    [ALERT] 'type' attribute is missing! This book will NOT appear in queries using TypeIndex.")
    except Exception as e:
        print(f"Error scanning table: {e}")

    # 3. Check Query via Index
    print("\n--- Query via TypeIndex (type='book') ---")
    try:
        from boto3.dynamodb.conditions import Key
        response = table.query(
            IndexName='TypeIndex',
            KeyConditionExpression=Key('type').eq('book'),
            Limit=5
        )
        items = response.get('Items', [])
        print(f"Found {len(items)} items via TypeIndex Query.")
        for item in items:
            print(f"  - {item.get('title')} (ID: {item.get('id')})")
    except Exception as e:
        print(f"Error querying index: {e}")

if __name__ == "__main__":
    verify_books()
