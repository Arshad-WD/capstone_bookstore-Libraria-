import pytest
import boto3
import os
from moto import mock_aws
from decimal import Decimal
from app_aws import SNSNotifier, DynamoBookRepository, DynamoUserRepository, DynamoOrderRepository, S3Uploader, setup_aws

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:BookBazaarNotifications"

@pytest.fixture
def dynamodb_mock(aws_credentials):
    with mock_aws():
        yield boto3.resource("dynamodb", region_name="us-east-1")

@pytest.fixture
def sns_mock(aws_credentials):
    with mock_aws():
        yield boto3.client("sns", region_name="us-east-1")

def test_setup_aws(dynamodb_mock, sns_mock):
    """Test that setup_aws creates the expected tables and topics."""
    # Ensure tables don't exist yet
    with pytest.raises(Exception):
        dynamodb_mock.Table('BookBazaarBooks').table_status
    
    # Run setup
    setup_aws()
    
    # Verify tables created
    assert dynamodb_mock.Table('BookBazaarBooks').table_status == 'ACTIVE'
    assert dynamodb_mock.Table('BookBazaarOrders').table_status == 'ACTIVE'
    
    # Verify SNS topic created (check by name in topics list)
    topics = sns_mock.list_topics()
    topic_arns = [t['TopicArn'] for t in topics['Topics']]
    assert any("BookBazaarNotifications" in arn for arn in topic_arns)

def test_dynamo_repo_add_and_get(dynamodb_mock):
    """Test adding and retrieving books from DynamoDB."""
    # Setup table
    dynamodb_mock.create_table(
        TableName='BookBazaarBooks',
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    
    repo = DynamoBookRepository()
    book_data = {
        'id': '1',
        'title': 'Test Book',
        'author': 'Test Author',
        'price': 29.99
    }
    
    # Test add
    success = repo.add(book_data)
    assert success is True
    
    # Test get_by_id
    retrieved = repo.get_by_id('1')
    assert retrieved is not None
    assert retrieved['title'] == 'Test Book'
    assert retrieved['price'] == Decimal('29.99')

def test_dynamo_repo_get_all(dynamodb_mock):
    """Test getting all books."""
    dynamodb_mock.create_table(
        TableName='BookBazaarBooks',
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    
    repo = DynamoBookRepository()
    repo.add({'id': '1', 'title': 'Book 1', 'price': 10})
    repo.add({'id': '2', 'title': 'Book 2', 'price': 20})
    
    books = repo.get_all()
    assert len(books) == 2
    titles = [b['title'] for b in books]
    assert 'Book 1' in titles
    assert 'Book 2' in titles

def test_dynamo_user_repo(dynamodb_mock):
    """Test user repository operations."""
    dynamodb_mock.create_table(
        TableName='BookBazaarUsers',
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    
    repo = DynamoUserRepository()
    user_data = {'id': 'u1', 'username': 'testuser', 'email': 'test@example.com'}
    
    # Test add
    assert repo.add(user_data) is True
    
    # Test get_by_email (simulated scan in repo)
    retrieved = repo.get_by_email('test@example.com')
    assert retrieved is not None
    assert retrieved['username'] == 'testuser'

def test_dynamo_order_repo(dynamodb_mock):
    """Test order repository operations."""
    dynamodb_mock.create_table(
        TableName='BookBazaarOrders',
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
    )
    
    repo = DynamoOrderRepository()
    order_data = {
        'id': 'o1',
        'book_id': 'b1',
        'seller_id': 's1',
        'total_price': 50.0
    }
    
    # Test add
    assert repo.add(order_data) is True
    
    # Test get_by_seller_id
    results = repo.get_by_seller_id('s1')
    assert len(results) == 1
    assert results[0]['id'] == 'o1'

@mock_aws
def test_s3_uploader(aws_credentials):
    """Test S3 file uploading."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket='bookbazaar-assets')
    
    # Create a dummy file
    test_file = "test_image.jpg"
    with open(test_file, "w") as f:
        f.write("dummy image data")
        
    try:
        uploader = S3Uploader()
        url = uploader.upload_file(test_file)
        
        assert url is not None
        assert "bookbazaar-assets.s3.us-east-1.amazonaws.com/test_image.jpg" in url
        
        # Verify file exists in s3
        response = s3.list_objects_v2(Bucket='bookbazaar-assets')
        assert response['KeyCount'] == 1
        assert response['Contents'][0]['Key'] == 'test_image.jpg'
    finally:
        if os.path.exists(test_file):
            os.remove(test_file)

def test_sns_notifier_send(sns_mock, mocker):
    """Test sending SNS notifications with publish verification."""
    # Create the topic
    topic = sns_mock.create_topic(Name='BookBazaarNotifications')
    topic_arn = topic['TopicArn']
    os.environ["SNS_TOPIC_ARN"] = topic_arn
    
    # Spy on the publish method of the mocked client
    spy = mocker.spy(sns_mock, 'publish')
    
    # Inject the mocked client into the notifier
    mock_aws_app = mocker.Mock()
    mock_aws_app.sns = sns_mock
    notifier = SNSNotifier(aws_instance=mock_aws_app)
    
    notifier.send("test@example.com", "Your order is ready")
    
    # Verify publish was called with correct parameters
    spy.assert_called_once()
    args, kwargs = spy.call_args
    assert kwargs['TopicArn'] == topic_arn
    assert kwargs['Message'] == "Your order is ready"
    assert kwargs['MessageAttributes']['email']['StringValue'] == "test@example.com"
