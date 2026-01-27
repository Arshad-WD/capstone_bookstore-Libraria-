from flask import Blueprint, render_template, redirect, url_for, session, flash, request
from app.repositories.book_repo import BookRepository
from app.repositories.order_repo import OrderRepository
from app.models.order import Order
from app.services.notification import NotificationService
from app.routes.auth import login_required

bookstore_bp = Blueprint("bookstore", __name__)
book_repo = BookRepository()
order_repo = OrderRepository()
notifier = NotificationService()

@bookstore_bp.route("/books", methods=["GET"])
@login_required
def books():
    """Display books with optional search filtering and pagination."""
    query = request.args.get('q', '')
    page = request.args.get('page', 1, type=int)
    per_page = 8  # Show 8 books per page
    
    if query:
        pagination = book_repo.search_paginated(query, page, per_page)
    else:
        pagination = book_repo.get_all_paginated(page, per_page)
    
    # Get cart count for display
    cart = session.get('cart', {})
    cart_count = sum(cart.values())
    
    return render_template("books.html", 
                         books=pagination.items, 
                         pagination=pagination,
                         username=session.get('username'),
                         query=query,
                         cart_count=cart_count)

@bookstore_bp.route("/cart/add/<int:book_id>", methods=["POST"])
@login_required
def add_to_cart(book_id):
    """Add a book to the session-based shopping cart."""
    book = book_repo.get_by_id(book_id)
    if not book:
        flash('Book not found.', 'error')
        return redirect(url_for('bookstore.books'))
    
    if book.stock < 1:
        flash('Sorry, this book is out of stock.', 'error')
        return redirect(url_for('bookstore.books'))
    
    cart = session.get('cart', {})
    book_id_str = str(book_id)
    cart[book_id_str] = cart.get(book_id_str, 0) + 1
    session['cart'] = cart
    session.modified = True
    
    flash(f'"{book.title}" added to cart.', 'success')
    return redirect(url_for('bookstore.books'))

@bookstore_bp.route("/cart")
@login_required
def view_cart():
    """Display the contents of the shopping cart."""
    cart = session.get('cart', {})
    cart_items = []
    total_price = 0
    
    for book_id_str, quantity in cart.items():
        book = book_repo.get_by_id(int(book_id_str))
        if book:
            item_total = book.price * quantity
            total_price += item_total
            cart_items.append({
                'book': book,
                'quantity': quantity,
                'item_total': item_total
            })
    
    return render_template("cart.html", cart_items=cart_items, total_price=total_price)

@bookstore_bp.route("/cart/remove/<int:book_id>", methods=["POST"])
@login_required
def remove_from_cart(book_id):
    """Remove a book from the cart."""
    cart = session.get('cart', {})
    book_id_str = str(book_id)
    if book_id_str in cart:
        del cart[book_id_str]
        session['cart'] = cart
        session.modified = True
        flash('Item removed from cart.', 'success')
    return redirect(url_for('bookstore.view_cart'))

@bookstore_bp.route("/cart/update/<int:book_id>", methods=["POST"])
@login_required
def update_cart(book_id):
    """Update quantity of a book in the cart."""
    try:
        quantity = int(request.form.get('quantity', 1))
        if quantity < 1:
            return remove_from_cart(book_id)
            
        book = book_repo.get_by_id(book_id)
        if quantity > book.stock:
            flash(f'Only {book.stock} units available.', 'warning')
            quantity = book.stock
            
        cart = session.get('cart', {})
        cart[str(book_id)] = quantity
        session['cart'] = cart
        session.modified = True
    except ValueError:
        pass
    return redirect(url_for('bookstore.view_cart'))

@bookstore_bp.route("/checkout", methods=["GET", "POST"])
@login_required
def checkout():
    """Handle checkout review (GET) and order finalization (POST)."""
    cart = session.get('cart', {})
    if not cart:
        flash('Your cart is empty.', 'error')
        return redirect(url_for('bookstore.books'))
    
    cart_items = []
    total_price = 0
    for book_id_str, quantity in cart.items():
        book = book_repo.get_by_id(int(book_id_str))
        if book:
            item_total = book.price * quantity
            total_price += item_total
            cart_items.append({
                'book': book,
                'quantity': quantity,
                'item_total': item_total
            })

    if request.method == "GET":
        return render_template("checkout.html", cart_items=cart_items, total_price=total_price)

    # POST logic - finalize order
    user_id = session.get('user_id')
    user_email = session.get('email')
    orders_placed = []
    
    try:
        for item in cart_items:
            book = item['book']
            quantity = item['quantity']
            
            if book.stock < quantity:
                flash(f'Issue with book "{book.title}": insufficient stock.', 'error')
                return redirect(url_for('bookstore.view_cart'))
            
            # Create order
            order = Order(
                user_id=user_id,
                book_id=book.id,
                quantity=quantity,
                total_price=item['item_total'],
                status='Placed'
            )
            order_repo.create(order)
            
            # Update stock
            book.stock -= quantity
            book_repo.update(book)
            orders_placed.append(book.title)
        
        # Clear cart
        session['cart'] = {}
        session.modified = True
        
        # Send notification
        notifier.send(user_email, f"Order placed for: {', '.join(orders_placed)}")
        flash('Your order has been placed successfully!', 'success')
        return redirect(url_for('auth.dashboard'))
            
    except Exception as e:
        flash('An error occurred during checkout.', 'error')
        return redirect(url_for('bookstore.view_cart'))

@bookstore_bp.route("/order/<int:book_id>", methods=["POST"])
@login_required
def place_order(book_id):
    """Refactored: Add single book to cart and go straight to checkout review."""
    try:
        book = book_repo.get_by_id(book_id)
        if not book:
            flash('Book not found.', 'error')
            return redirect(url_for('bookstore.books'))
        
        if book.stock < 1:
            flash('Sorry, this book is out of stock.', 'error')
            return redirect(url_for('bookstore.books'))
        
        # Add to cart (or replace cart if we want "direct buy" to be exclusive, 
        # but usually it just adds and goes to checkout)
        cart = session.get('cart', {})
        book_id_str = str(book_id)
        cart[book_id_str] = cart.get(book_id_str, 0) + 1
        session['cart'] = cart
        session.modified = True
        
        # Instead of placing order, redirect to checkout review
        return redirect(url_for('bookstore.checkout'))
    
    except Exception as e:
        flash('An error occurred.', 'error')
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

@bookstore_bp.route("/order/cancel/<int:order_id>", methods=["POST"])
@login_required
def cancel_order(order_id):
    """Allow user to cancel a placed order."""
    try:
        user_id = session.get('user_id')
        order = order_repo.get_by_id(order_id)
        
        if not order or order.user_id != user_id:
            flash('Order not found.', 'error')
            return redirect(url_for('auth.dashboard'))
        
        if order.status != 'Placed':
            flash('This order cannot be cancelled as it is already being processed.', 'warning')
            return redirect(url_for('auth.dashboard'))
        
        # Update order status
        order.status = 'Cancelled'
        order_repo.update(order)
        
        # Restore book stock
        book = book_repo.get_by_id(order.book_id)
        if book:
            book.stock += order.quantity
            book_repo.update(book)
        
        # Send notification
        user_email = session.get('email')
        notifier.send(user_email, f"Order #{order.id} for {book.title if book else 'your item'} has been cancelled.")
        
        flash(f'Order #{order.id} cancelled successfully.', 'success')
        return redirect(url_for('auth.dashboard'))
    
    except Exception as e:
        flash('An error occurred while cancelling your order.', 'error')
        return redirect(url_for('auth.dashboard'))
