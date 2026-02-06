from flask import Flask
from .extensions import db
import os
from datetime import timedelta

def create_app():
    app = Flask(__name__)
    
    # Load config from root config.py
    from config import config
    config_name = os.environ.get('FLASK_ENV', 'development')
    app.config.from_object(config.get(config_name, config['default']))
    
    # Additional session configuration
    app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=24)
    
    # Initialize database
    db.init_app(app)
    
    # Register blueprints
    from .routes.auth import auth_bp
    from .routes.bookstore import bookstore_bp
    from .routes.admin import admin_bp
    from .routes.seller import seller_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(bookstore_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(seller_bp)
    
    # Create tables if they don't exist
    with app.app_context():
        db.create_all()
    
    return app