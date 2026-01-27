from app.extensions import db
from app.models.order import Order

class OrderRepository:
    def create(self, order):
        """Create a new order."""
        db.session.add(order)
        db.session.commit()
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
        