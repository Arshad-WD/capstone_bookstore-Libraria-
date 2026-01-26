from flask import Blueprint, render_template, redirect, url_for, session, flash
from app.extensions import db
from app.models.user import User
from app.models.book import Book
from app.models.order import Order
from app.routes.auth import login_required
from functools import wraps
from sqlalchemy import func

admin_bp = Blueprint("admin", __name__, url_prefix="/admin")

def admin_required(f):
    """Decorator to require admin role for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('auth.login'))
        
        user = User.query.get(session.get('user_id'))
        if not user or user.role != 'admin':
            flash('Access denied. Admin privileges required.', 'error')
            return redirect(url_for('bookstore.books'))
        
        return f(*args, **kwargs)
    return decorated_function

@admin_bp.route("/dashboard")
@admin_required
def dashboard():
    """Admin dashboard with statistics and tracking."""
    
    # Get statistics
    total_users = User.query.filter_by(role='customer').count()
    total_books = Book.query.count()
    total_orders = Order.query.count()
    
    # Calculate total revenue
    total_revenue = db.session.query(func.sum(Order.total_price)).scalar() or 0
    
    # Get recent orders (last 10)
    recent_orders = Order.query.order_by(Order.order_date.desc()).limit(10).all()
    
    # Get low stock books (stock < 10)
    low_stock_books = Book.query.filter(Book.stock < 10).order_by(Book.stock.asc()).all()
    
    # Get books by status
    out_of_stock = Book.query.filter_by(stock=0).count()
    in_stock = Book.query.filter(Book.stock > 0).count()
    
    # Get top selling books (books with most orders)
    top_books = db.session.query(
        Book.title,
        Book.author,
        func.count(Order.id).label('order_count')
    ).join(Order).group_by(Book.id).order_by(func.count(Order.id).desc()).limit(5).all()
    
    # Get order breakdown by status
    order_status_counts = db.session.query(
        Order.status,
        func.count(Order.id).label('count')
    ).group_by(Order.status).all()
    
    stats = {
        'total_users': total_users,
        'total_books': total_books,
        'total_orders': total_orders,
        'total_revenue': total_revenue,
        'out_of_stock': out_of_stock,
        'in_stock': in_stock
    }
    
    return render_template(
        "admin_dashboard.html",
        stats=stats,
        recent_orders=recent_orders,
        low_stock_books=low_stock_books,
        top_books=top_books,
        order_status_counts=order_status_counts,
        username=session.get('username')
    )

@admin_bp.route("/users")
@admin_required
def users():
    """View all users."""
    all_users = User.query.all()
    return render_template("admin_users.html", users=all_users, username=session.get('username'))

@admin_bp.route("/books")
@admin_required
def books():
    """View all books with stock management."""
    all_books = Book.query.order_by(Book.title).all()
    return render_template("admin_books.html", books=all_books, username=session.get('username'))

@admin_bp.route("/orders")
@admin_required
def orders():
    """View all orders."""
    all_orders = Order.query.order_by(Order.order_date.desc()).all()
    return render_template("admin_orders.html", orders=all_orders, username=session.get('username'))
