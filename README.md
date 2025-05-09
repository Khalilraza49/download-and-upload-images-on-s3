Install dependencies:
pip install boto3 requests
Set up AWS credentials via environment variables or ~/.aws/credentials.

Security Note:
Make sure your AWS credentials are not hardcoded in the script. Use environment variables, an .env file, or IAM roles if running on AWS infrastructure.
