import os
import duckdb
import b2sdk.v2 as b2
from dotenv import load_dotenv, find_dotenv

def create_duckdb_with_b2_data(bucket_name, prefix, db_path="b2_data.duckdb", table_name="b2_data"):
    """
    Create a DuckDB database that directly queries data from a B2 bucket.
    
    Args:
        bucket_name: Name of the B2 bucket
        prefix: Path prefix in bucket where data is stored
        db_path: Path to save the DuckDB database
        table_name: Name of the table to create
    """
    # Load environment variables
    load_dotenv(find_dotenv())
    
    # Get B2 credentials
    key_id = os.getenv("B2_KEY_ID")
    app_key = os.getenv("B2_APP_KEY")
    endpoint = os.getenv("B2_S3_ENDPOINT_URL", "s3.us-east-005.backblazeb2.com")
    
    # Remove https:// if present
    if endpoint.startswith("https://"):
        endpoint = endpoint[8:]
    
    if not all([key_id, app_key, bucket_name]):
        raise ValueError("Missing B2 credentials or bucket name")
    
    # Connect to B2 to list files
    print("Connecting to B2...")
    info = b2.InMemoryAccountInfo()
    b2_api = b2.B2Api(info)
    b2_api.authorize_account("production", key_id, app_key)
    bucket = b2_api.get_bucket_by_name(bucket_name)
    
    # List parquet files
    print(f"Listing files in {bucket_name}/{prefix}...")
    parquet_files = []
    for file_info, _ in bucket.ls(folder_to_list=prefix):
        if file_info.file_name.endswith('.parquet'):
            parquet_files.append(file_info.file_name)
    
    print(f"Found {len(parquet_files)} parquet files")
    
    # Create S3 URLs for the parquet files
    s3_urls = [f"s3://{bucket_name}/{file_name}" for file_name in parquet_files]
    
    # Initialize DuckDB and load httpfs extension
    print(f"Creating DuckDB database: {db_path}")
    conn = duckdb.connect(database=db_path)
    
    # Install and load httpfs extension
    print("Setting up httpfs extension...")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    
    # Configure S3 settings
    region = endpoint.split('.')[1] if len(endpoint.split('.')) > 2 else "us-west-001"
    conn.execute(f"SET s3_region='{region}';")
    conn.execute(f"SET s3_access_key_id='{key_id}';")
    conn.execute(f"SET s3_secret_access_key='{app_key}';")
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_url_style='path';")
    
    # Additional settings to improve reliability
    conn.execute("SET enable_http_metadata_cache=false;")
    conn.execute("SET enable_object_cache=false;")
    conn.execute("SET http_timeout=30000;")  # 30 seconds timeout
    
    # Create table from direct S3 URLs
    urls_str = ", ".join([f"'{url}'" for url in s3_urls])
    create_table_sql = f"""
    CREATE TABLE {table_name} AS 
    SELECT * FROM parquet_scan([{urls_str}]);
    """
    
    print(f"Creating table '{table_name}' by directly querying {len(s3_urls)} files...")
    try:
        conn.execute(create_table_sql)
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"Successfully created table '{table_name}' with {row_count} rows")
    except Exception as e:
        print(f"Error creating table directly: {str(e)}")
        print("DuckDB may not be able to directly access the B2 bucket via S3. Consider using the download approach instead.")
    
    # Close the connection
    conn.close()
    
    print(f"Database created at: {os.path.abspath(db_path)}")
    return os.path.abspath(db_path)

if __name__ == "__main__":
    bucket_name = os.getenv("B2_BUCKET_NAME") or "test-duckdb"
    prefix = "data/chunked"
    db_path = "b2_data.duckdb"
    
    db_file = create_duckdb_with_b2_data(bucket_name, prefix, db_path)
    print(f"\nYou can now use the database with: duckdb.connect('{db_file}')")