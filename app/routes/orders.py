from flask import Blueprint, redirect, render_template
from app.repositories.order_repo import OrderRepository
from app.models.order import Order
from app.services.notification import NotificationService

orders_bp = Blueprint("orders", __name__)
repo = OrderRepository()
notifier = NotificationService()

@orders_bp.route("/order/<book_title>")
def place_order(book_title):
    order = Order(
        user_email="demo@user.com",
        book_title=book_title,
        status="Placed"
    )
    repo.create(order)
    notifier.send("demo@user.com", f"Order placed for {book_title}")
    return redirect("/order-success")

@orders_bp.route("/order-success")
def success():
    return render_template("order_success.html")
