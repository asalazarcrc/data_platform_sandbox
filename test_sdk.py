import os
from dotenv import load_dotenv, find_dotenv
from b2sdk.v2 import *

# Load environment variables
load_dotenv(find_dotenv())

# Print environment variables (masked for security)
key_id = os.getenv("B2_KEY_ID")
app_key = os.getenv("B2_APP_KEY")
bucket_name = os.getenv("B2_BUCKET_NAME")

print(f"Key ID: {key_id[:4]}... (first 4 chars only)")
print(f"App Key: {app_key[:4]}... (first 4 chars only)")
print(f"Bucket Name: {bucket_name}")


try:
    print("\nInitializing B2 API...")
    # Create the B2 info object
    info = InMemoryAccountInfo()
    
    # Create the B2 API client
    b2_api = B2Api(info)
    
    # Authorize account
    print("Authorizing with B2...")
    b2_api.authorize_account("production", key_id, app_key)
    
    print("\nConnection established successfully!")
    
    # List buckets
    print("Listing buckets...")
    for bucket in b2_api.list_buckets():
        print(f"Found bucket: {bucket.name} (ID: {bucket.id_})")
    
    # Try to access the specific bucket
    if bucket_name:
        print(f"\nTrying to access specific bucket: {bucket_name}")
        bucket = b2_api.get_bucket_by_name(bucket_name)
        print(f"Successfully accessed bucket: {bucket.name}")
        
        # List files
        print("\nListing files (up to 10)...")
        file_count = 0
        for file_version, _ in bucket.ls(limit=10):
            print(f"Found file: {file_version.file_name}")
            file_count += 1
        
        if file_count == 0:
            print("No files found in this bucket")
    
    print("\nNative B2 API test SUCCESSFUL")
    
except Exception as e:
    print(f"\nNative B2 API test FAILED: {str(e)}")