from flask import Blueprint, render_template, redirect, url_for, session, flash, request
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
    total_users = User.query.filter_by(role='buyer').count()
    total_sellers = User.query.filter_by(role='seller').count()
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
        'total_sellers': total_sellers,
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
    """View all users or filter by role."""
    role_filter = request.args.get('role')
    if role_filter in ['seller', 'buyer', 'admin']:
        display_users = User.query.filter_by(role=role_filter).all()
    else:
        display_users = User.query.all()
        role_filter = 'all'
        
    return render_template("admin_users.html", 
                         users=display_users, 
                         current_role=role_filter,
                         username=session.get('username'))

@admin_bp.route("/books")
@admin_required
def books():
    """View all books with stock management, search, and pagination."""
    from app.repositories.book_repo import BookRepository
    book_repo = BookRepository()
    
    query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)
    per_page = 10
    
    if query:
        pagination = book_repo.search_paginated(query, page, per_page)
    else:
        pagination = book_repo.get_all_paginated(page, per_page)
        
    return render_template("admin_books.html", 
                         books=pagination.items, 
                         pagination=pagination,
                         query=query,
                         username=session.get('username'))

@admin_bp.route("/orders")
@admin_required
def orders():
    """View all orders."""
    all_orders = Order.query.order_by(Order.order_date.desc()).all()
    return render_template("admin_orders.html", orders=all_orders, username=session.get('username'))

@admin_bp.route("/books/add", methods=["POST"])
@admin_required
def add_book():
    """Add a new book to the database."""
    try:
        title = request.form.get("title")
        author = request.form.get("author")
        description = request.form.get("description")
        price = float(request.form.get("price", 0))
        stock = int(request.form.get("stock", 0))
        image_url = request.form.get("image_url", "")

        if not title or not author:
            flash("Title and Author are required.", "error")
            return redirect(url_for("admin.books"))

        new_book = Book(
            title=title,
            author=author,
            description=description,
            price=price,
            stock=stock,
            image_url=image_url
        )
        db.session.add(new_book)
        db.session.commit()
        
        flash(f'Book "{title}" added successfully!', "success")
        return redirect(url_for("admin.books"))
    except Exception as e:
        flash("An error occurred while adding the book.", "error")
        return redirect(url_for("admin.books"))

@admin_bp.route("/books/update_stock/<int:book_id>", methods=["POST"])
@admin_required
def update_stock(book_id):
    """Refined: Update stock for an existing book (Add or Set)."""
    try:
        book = Book.query.get(book_id)
        if not book:
            flash("Book not found.", "error")
            return redirect(url_for("admin.books"))

        amount = int(request.form.get("stock", 0))
        action = request.form.get("action", "set")  # Default to 'set' for safety
        
        if action == "add":
            book.stock += amount
            message = f'Added {amount} units to "{book.title}". Total: {book.stock}'
        else:
            book.stock = amount
            message = f'Stock updated for "{book.title}" to {amount} units.'
            
        db.session.commit()
        flash(message, "success")
        return redirect(url_for("admin.books"))
    except Exception as e:
        flash("An error occurred while updating stock.", "error")
        return redirect(url_for("admin.books"))

@admin_bp.route("/users/promote/<int:user_id>", methods=["POST"])
@admin_required
def promote_user(user_id):
    """Promote a customer to admin."""
    try:
        user = User.query.get(user_id)
        if not user:
            flash("User not found.", "error")
            return redirect(url_for("admin.users"))
            
        if user.role == 'admin':
            flash(f"User {user.username} is already an admin.", "warning")
            return redirect(url_for("admin.users"))
            
        user.role = 'admin'
        db.session.commit()
        flash(f"User {user.username} promoted to Admin successfully!", "success")
        return redirect(url_for("admin.users"))
    except Exception as e:
        flash("An error occurred while promoting user.", "error")
        return redirect(url_for("admin.users"))

@admin_bp.route("/users/promote_seller/<int:user_id>", methods=["POST"])
@admin_required
def promote_seller(user_id):
    """Promote a customer to seller."""
    try:
        user = User.query.get(user_id)
        if not user:
            flash("User not found.", "error")
            return redirect(url_for("admin.users"))
            
        if user.role != 'buyer':
            flash(f"Only buyers can be promoted to seller.", "warning")
            return redirect(url_for("admin.users"))
            
        user.role = 'seller'
        db.session.commit()
        flash(f"User {user.username} promoted to Seller successfully!", "success")
        return redirect(url_for("admin.users"))
    except Exception as e:
        flash("An error occurred while promoting user.", "error")
        return redirect(url_for("admin.users"))

@admin_bp.route("/users/revoke/<int:user_id>", methods=["POST"])
@admin_required
def revoke_admin(user_id):
    """Revoke admin role from a user."""
    try:
        user = User.query.get(user_id)
        if not user:
            flash("User not found.", "error")
            return redirect(url_for("admin.users"))
            
        # Self-protection: cannot revoke own role
        if user.id == session.get('user_id'):
            flash("You cannot revoke your own admin status.", "error")
            return redirect(url_for("admin.users"))
            
        if user.role not in ['admin', 'seller']:
            flash(f"User {user.username} is already a buyer.", "warning")
            return redirect(url_for("admin.users"))
            
        user.role = 'buyer'
        db.session.commit()
        flash(f"Admin status revoked for user {user.username}.", "success")
        return redirect(url_for("admin.users"))
    except Exception as e:
        flash("An error occurred while revoking admin status.", "error")
        return redirect(url_for("admin.users"))

@admin_bp.route("/users/validate/<int:user_id>", methods=["POST"])
@admin_required
def validate_user(user_id):
    """Approve/Validate a seller's credentials."""
    try:
        user = User.query.get(user_id)
        if not user:
            flash("User not found.", "error")
            return redirect(url_for("admin.users"))
            
        user.is_validated = not user.is_validated
        db.session.commit()
        
        status = "validated" if user.is_validated else "unvalidated"
        flash(f"User {user.username} is now {status}.", "success")
        return redirect(url_for("admin.users"))
    except Exception as e:
        flash("An error occurred while validating user.", "error")
        return redirect(url_for("admin.users"))

@admin_bp.route("/users/bulk_promote_sellers", methods=["POST"])
@admin_required
def bulk_promote_sellers():
    """Promote all buyers to validated sellers."""
    try:
        # Update all buyers to sellers and validate them
        affected = User.query.filter_by(role='buyer').update({
            "role": 'seller',
            "is_validated": True
        })
        db.session.commit()
        flash(f"Success! {affected} users promoted to Validated Sellers.", "success")
        return redirect(url_for("admin.users"))
    except Exception as e:
        db.session.rollback()
        flash("An error occurred while bulk promoting users.", "error")
        return redirect(url_for("admin.users"))

@admin_bp.route("/users/bulk_reset_buyers", methods=["POST"])
@admin_required
def bulk_reset_buyers():
    """Reset all non-admins to buyers."""
    try:
        current_admin_id = session.get('user_id')
        # Update all users except the current admin to buyers
        affected = User.query.filter(User.id != current_admin_id).update({
            "role": 'buyer',
            "is_validated": False
        })
        db.session.commit()
        flash(f"Success! {affected} users reset to Buyers.", "success")
        return redirect(url_for("admin.users"))
    except Exception as e:
        db.session.rollback()
        flash("An error occurred while bulk resetting users.", "error")
        return redirect(url_for("admin.users"))
