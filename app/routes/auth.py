from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from app.services.auth_service import AuthService
from functools import wraps

auth_bp = Blueprint("auth", __name__)
service = AuthService()

def login_required(f):
    """Decorator to require login for routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'error')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

@auth_bp.route("/", methods=["GET"])
def home():
    if 'user_id' in session:
        return redirect(url_for('bookstore.books'))
    return redirect(url_for('auth.login'))

@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        try:
            user = service.register(request.form)
            flash('Registration successful! Please log in.', 'success')
            return redirect(url_for('auth.login'))
        except Exception as e:
            flash(str(e), 'error')
            return render_template("register.html")

    return render_template("register.html")

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = service.login(
            request.form.get("email"),
            request.form.get("password")
        )

        if user:
            session['user_id'] = user.id
            session['username'] = user.username
            session['email'] = user.email
            session['user_role'] = user.role
            session.permanent = True
            flash(f'Welcome back, {user.username}!', 'success')
            
            # Redirect admin to admin dashboard
            if user.role == 'admin':
                return redirect(url_for('admin.dashboard'))
            
            return redirect(url_for('bookstore.books'))

        flash('Invalid email or password', 'error')
        return render_template("login.html")

    return render_template("login.html")

@auth_bp.route("/logout")
def logout():
    session.clear()
    flash('You have been logged out successfully.', 'success')
    return redirect(url_for('auth.login'))

@auth_bp.route("/dashboard")
@login_required
def dashboard():
    from app.repositories.order_repo import OrderRepository
    from app.models.user import User
    
    order_repo = OrderRepository()
    user_id = session.get('user_id')
    orders = order_repo.get_user_orders(user_id)
    
    return render_template("dashboard.html", orders=orders, username=session.get('username'))
