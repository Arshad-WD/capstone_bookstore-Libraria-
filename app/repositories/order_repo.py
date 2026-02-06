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
        """Get all orders for a specific user."""
        return Order.query.filter_by(user_id=user_id).order_by(Order.order_date.desc()).all()

    def update(self, order):
        """Update an existing order."""
        db.session.commit()
        return order
        