
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add current directory to path
sys.path.append(os.getcwd())

from app_aws import DynamoBookRepository, DynamoUserRepository, aws_app

def test_dynamodb_access():
    print("Testing DynamoDB Access...")
    
    # 1. Check User Data
    print("\n--- Checking Users ---")
    user_repo = DynamoUserRepository()
    # Try to scan a few users
    try:
        response = user_repo.table.scan(Limit=5)
        items = response.get('Items', [])
        print(f"Found {len(items)} users.")
        for item in items:
            print(f"  - {item.get('username')} (ID: {item.get('id')})")
    except Exception as e:
        print(f"Error scanning users: {e}")

    # 2. Check Book Data via Repository
    print("\n--- Checking Books (get_paginated) ---")
    book_repo = DynamoBookRepository()
    try:
        result = book_repo.get_paginated(limit=5)
        items = result['Items']
        print(f"Found {len(items)} books via get_paginated.")
        for item in items:
            print(f"  - {item.get('title')} (ID: {item.get('id')}, Type: {item.get('type')})")
            
        if not items:
            print("No items returned from get_paginated. Checking raw scan...")
            scan_response = book_repo.table.scan(Limit=5)
            scan_items = scan_response.get('Items', [])
            print(f"Raw scan found {len(scan_items)} books.")
            for item in scan_items:
                 print(f"  - {item.get('title')} (ID: {item.get('id')}, Type: {item.get('type')})")

    except Exception as e:
        print(f"Error fetching books: {e}")

if __name__ == "__main__":
    test_dynamodb_access()
