from flask import Blueprint, render_template, redirect, url_for, session, flash
from app.repositories.book_repo import BookRepository
from app.repositories.order_repo import OrderRepository
from app.models.order import Order
from app.services.notification import LocalNotifier
from app.routes.auth import login_required

bookstore_bp = Blueprint("bookstore", __name__)
book_repo = BookRepository()
order_repo = OrderRepository()
notifier = LocalNotifier()

@bookstore_bp.route("/books", methods=["GET"])
@login_required
def books():
    """Display all available books in a grid layout."""
    all_books = book_repo.get_all()
    return render_template("books.html", books=all_books, username=session.get('username'))

@bookstore_bp.route("/order/<int:book_id>", methods=["POST"])
@login_required
def place_order(book_id):
    """Place an order for a specific book."""
    try:
        user_id = session.get('user_id')
        book = book_repo.get_by_id(book_id)
        
        if not book:
            flash('Book not found.', 'error')
            return redirect(url_for('bookstore.books'))
        
        if book.stock < 1:
            flash('Sorry, this book is out of stock.', 'error')
            return redirect(url_for('bookstore.books'))
        
        # Create order
        order = Order(
            user_id=user_id,
            book_id=book_id,
            quantity=1,
            total_price=book.price,
            status='Placed'
        )
        
        created_order = order_repo.create(order)
        
        # Update stock
        book.stock -= 1
        book_repo.update(book)
        
        # Send notification
        user_email = session.get('email')
        notifier.send(user_email, f"Order placed for {book.title}")
        
        flash(f'Order placed successfully for "{book.title}"!', 'success')
        return redirect(url_for('bookstore.order_confirmation', order_id=created_order.id))
    
    except Exception as e:
        flash('An error occurred while placing your order.', 'error')
        return redirect(url_for('bookstore.books'))

@bookstore_bp.route("/order/confirmation/<int:order_id>")
@login_required
def order_confirmation(order_id):
    """Display order confirmation page."""
    order = order_repo.get_by_id(order_id)
    
    if not order or order.user_id != session.get('user_id'):
        flash('Order not found.', 'error')
        return redirect(url_for('bookstore.books'))
    
    return render_template("order_confirmation.html", order=order)
