from flask import Blueprint, render_template, redirect, url_for, session, flash, request
from app.extensions import db
from app.models.user import User
from app.models.book import Book
from app.models.order import Order
from app.routes.auth import login_required
from functools import wraps
from app_aws import aws_app

seller_bp = Blueprint("seller", __name__, url_prefix="/seller")

def seller_required(f):
    """Decorator to require seller role for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('auth.login'))
        
        user = User.query.get(session.get('user_id'))
        if not user or user.role != 'seller':
            flash('Access denied. Seller privileges required.', 'error')
            return redirect(url_for('bookstore.books'))
        
        return f(*args, **kwargs)
    return decorated_function

@seller_bp.route("/dashboard")
@seller_required
def dashboard():
    """Seller dashboard with their own books."""
    user_id = session.get('user_id')
    my_books = Book.query.filter_by(seller_id=user_id).all()
    
    # Calculate some stats for the seller
    total_stock = sum(book.stock for book in my_books)
    
    return render_template(
        "seller_dashboard.html",
        books=my_books,
        total_stock=total_stock,
        user=User.query.get(user_id),
        username=session.get('username')
    )

@seller_bp.route("/books/add", methods=["POST"])
@seller_required
def add_book():
    """Allow seller to add their own book."""
    try:
        title = request.form.get("title")
        author = request.form.get("author")
        description = request.form.get("description")
        price = float(request.form.get("price", 0))
        stock = int(request.form.get("stock", 0))
        image_url = request.form.get("image_url", "")
        seller_id = session.get('user_id')

        if not title or not author:
            flash("Title and Author are required.", "error")
            return redirect(url_for("seller.dashboard"))

        # IAM Permission Check (Simulation)
        if not aws_app.check_iam_permission('seller', 'books:add'):
            flash("IAM Policy restriction: Access denied.", "error")
            return redirect(url_for("seller.dashboard"))
            
        # S3 Simulation: REMOVED for Zero-S3 Architecture
        # The app now uses local static paths or direct URLs

        new_book = Book(
            title=title,
            author=author,
            description=description,
            price=price,
            stock=stock,
            image_url=image_url,
            seller_id=seller_id
        )
        db.session.add(new_book)
        db.session.commit()
        
        flash(f'Book "{title}" added successfully!', "success")
        return redirect(url_for("seller.dashboard"))
    except Exception as e:
        flash("An error occurred while adding the book.", "error")
        return redirect(url_for("seller.dashboard"))

@seller_bp.route("/books/delete/<int:book_id>", methods=["POST"])
@seller_required
def delete_book(book_id):
    """Allow seller to delete their own book."""
    try:
        user_id = session.get('user_id')
        book = Book.query.filter_by(id=book_id, seller_id=user_id).first()
        
        if not book:
            flash("Book not found or access denied.", "error")
            return redirect(url_for("seller.dashboard"))
            
        db.session.delete(book)
        db.session.commit()
        flash(f'Book deleted successfully.', 'success')
        return redirect(url_for("seller.dashboard"))
    except Exception as e:
        flash("An error occurred while deleting the book.", "error")
        return redirect(url_for("seller.dashboard"))

@seller_bp.route("/sales")
@seller_required
def sales():
    """View orders for books owned by the seller."""
    try:
        user_id = session.get('user_id')
        
        # Get all books owned by this seller
        my_book_ids = [b.id for b in Book.query.filter_by(seller_id=user_id).all()]
        
        # Get all orders for those books
        my_sales = Order.query.filter(Order.book_id.in_(my_book_ids)).order_by(Order.order_date.desc()).all()
        
        # Calculate total revenue for this seller
        total_revenue = sum(sale.total_price for sale in my_sales)
        
        return render_template(
            "seller_orders.html",
            sales=my_sales,
            total_revenue=total_revenue,
            user=User.query.get(user_id),
            username=session.get('username')
        )
    except Exception as e:
        flash("An error occurred while fetching sales data.", "error")
        return redirect(url_for("seller.dashboard"))
