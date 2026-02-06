from app.extensions import db
from app.models.order import Order
from app_aws import DynamoOrderRepository

class OrderRepository:
    def create(self, order):
        """Create a new order in SQL and DynamoDB."""
        db.session.add(order)
        db.session.commit()
        
        # Sync to DynamoDB
        try:
            dynamo = DynamoOrderRepository()
            dynamo.add({
                'id': str(order.id),
                'user_id': str(order.user_id),
                'book_id': str(order.book_id),
                'seller_id': str(order.book.seller_id) if order.book and order.book.seller_id else "system",
                'quantity': order.quantity,
                'total_price': order.total_price,
                'status': order.status,
                'order_date': order.order_date.isoformat()
            })
        except Exception as e:
            print(f"DynamoDB Sync Error: {e}")
            
        return order
    
    def get_by_id(self, order_id):
        """Get an order by ID."""
        return Order.query.get(order_id)
    
    def get_user_orders(self, user_id):
        """Get orders from DynamoDB."""
        try:
            # Note: For efficiency, this would use a GSI. 
            # Current implementation uses scan (from app_aws.py)
            dynamo = DynamoOrderRepository()
            # Since app_aws.py scan doesn't have a get_by_user_id, we'll scan all for demo
            # In production, you'd call a query on a UserOrdersIndex
            items = dynamo.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('user_id').eq(str(user_id))
            ).get('Items', [])
            
            orders = []
            for item in items:
                order = Order(
                    user_id=item.get('user_id'),
                    book_id=item.get('book_id'),
                    quantity=int(item.get('quantity', 1)),
                    total_price=float(item.get('total_price', 0)),
                    status=item.get('status', 'Placed')
                )
                order.id = item.get('id')
                # Date parsing
                try:
                    order.order_date = datetime.fromisoformat(item.get('order_date'))
                except:
                    order.order_date = datetime.utcnow()
                orders.append(order)
            return orders
        except Exception as e:
            print(f"DynamoDB Order Read Error: {e}")
            return Order.query.filter_by(user_id=user_id).order_by(Order.order_date.desc()).all()

    def update(self, order):
        """Update an existing order."""
        db.session.commit()
        return order
        