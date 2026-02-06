import os
from datetime import timedelta

class Config:
    """Base configuration class with common settings."""
    
    # Application settings
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    # Database settings - SQLite for local development, MySQL for production
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    
    # Check for MySQL first (RDS/EC2)
    MYSQL_USER = os.environ.get('MYSQL_USER')
    MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
    MYSQL_DB = os.environ.get('MYSQL_DB')
    MYSQL_HOST = os.environ.get('MYSQL_HOST', 'localhost')
    
    if MYSQL_USER and MYSQL_PASSWORD and MYSQL_DB:
        SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
    else:
        SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
            'sqlite:///' + os.path.join(BASE_DIR, 'bookbazaar.db')
            
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Session settings
    SESSION_TYPE = 'filesystem'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    
    # AWS Configuration placeholders (for future migration)
    # AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
    # DYNAMODB_TABLE_PREFIX = os.environ.get('DYNAMODB_TABLE_PREFIX', 'bookbazaar')
    # SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
    
class DevelopmentConfig(Config):
    """Development environment configuration."""
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    """Production environment configuration."""
    DEBUG = False
    TESTING = False
    # In production, SECRET_KEY must be set via environment variable
    
class TestingConfig(Config):
    """Testing environment configuration."""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}
