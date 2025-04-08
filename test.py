import polars as pl
import pyodbc
import os
from dotenv import load_dotenv, find_dotenv
from utils.funcs import blackswan_sql_conn
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
import math

# Load environment variables from .env file
load_dotenv(find_dotenv())

print(os.getenv("B2_S3_ENDPOINT"))
print(os.getenv("B2_KEY_ID"))
print(os.getenv("B2_APP_KEY"))
print(os.getenv("B2_S3_BUCKET_NAME"))

def test_s3_connection(s3_resource, bucket_name):
    """Test the S3 connection by listing buckets"""
    try:
        print("Testing S3 connection...")
        # List buckets to test connection
        for bucket in s3_resource.buckets.all():
            print(f"Found bucket: {bucket.name}")
        
        # Try to access the specific bucket
        bucket = s3_resource.Bucket(bucket_name)
        
        # List a few objects to ensure access (limit to 5 for brevity)
        count = 0
        for obj in bucket.objects.limit(5):
            print(f"Found object: {obj.key}")
            count += 1
        
        if count == 0:
            print(f"Bucket {bucket_name} exists but appears to be empty")
        
        print("S3 connection test successful")
        return True
    except Exception as e:
        print(f"S3 connection test failed: {str(e)}")
        return False
    
# Configure the B2 connection

s3 = boto3.resource(
    's3',
    endpoint_url=os.getenv("B2_S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("B2_KEY_ID"),
    aws_secret_access_key=os.getenv("B2_APP_KEY"),
    config=Config(signature_version='s3v4')
)

bucket_name = os.getenv("B2_S3_BUCKET_NAME")
# Test the connection

if not test_s3_connection(s3, bucket_name):
    raise Exception("Failed to connect to S3. Check your credentials and bucket name.")