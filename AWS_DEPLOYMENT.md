# 🚀 AWS Deployment Guide (Production)

Steps to transition the BookBazaar platform from local development to AWS Cloud.

## 1. Prerequisites
- **AWS Account**: Active account with administrative access.
- **AWS CLI**: Installed and configured (`aws configure`).
- **Python 环境**: Python 3.9+ installed on the target server.

## 2. Infrastructure Setup

### A. Database (SQL Layer)
1.  **Amazon RDS (MySQL)**: Create a MySQL instance.
2.  **Security Group**: Allow TCP port 3306 from your application server's IP.
3.  **Credentials**: Save the endpoint, username, password, and database name.

### B. Cloud Services (AWS Native)
Run the built-in utility to automate resource creation:
```bash
python app_aws.py setup
```
This will automatically create:
- **DynamoDB Tables**: `BookBazaarBooks`, `BookBazaarUsers`, `BookBazaarOrders`.
- **SNS Topic**: `BookBazaarNotifications`.
- **S3 Bucket**: `bookbazaar-assets`.

## 3. Configuration (.env)
Update your production `.env` with the new cloud endpoints:
```ini
# Database
MYSQL_USER=admin
MYSQL_PASSWORD=your_rds_password
MYSQL_HOST=your-rds-endpoint.aws.com
MYSQL_DB=bookbazaar

# AWS
AWS_REGION=us-east-1
SNS_TOPIC_ARN=arn:aws:sns:us-east-1:123456789012:BookBazaarNotifications
S3_BUCKET_NAME=bookbazaar-assets
```

## 4. Application Deployment
You can deploy using **Elastic Beanstalk** (Recommended for Flask) or **EC2**.

### Option: Elastic Beanstalk (EB CLI)
1.  `eb init` (Select Python platform).
2.  `eb create bookbazaar-prod`.
3.  Add environment variables in the EB Console (matching your `.env`).

## 5. Final Checklist
- [ ] Run `python seed_data.py` on production to load initial catalog.
- [ ] Verify `verify_aws` command: `python app_aws.py verify`.
- [ ] Ensure `SECRET_KEY` is a long, random string.
