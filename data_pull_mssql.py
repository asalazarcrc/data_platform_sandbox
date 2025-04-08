import polars as pl
import pyodbc
import os
import math
import sys
from dotenv import load_dotenv, find_dotenv
from utils.funcs import blackswan_sql_conn
import b2sdk.v2 as b2

# Load environment variables from .env file
load_dotenv(find_dotenv())

def saved_chunked_parquet_b2(df, bucket_name, prefix, target_size_gb=1):
    """
    Save a Polars DataFrame as chunked parquet files to Backblaze B2 using native B2 SDK.
    
    Args:
        df: Polars DataFrame to save
        bucket_name: Name of the B2 bucket
        prefix: Path prefix within the bucket
        target_size_gb: Target size of each chunk in GB
    """
    # Get B2 credentials from environment
    key_id = os.getenv("B2_KEY_ID")
    app_key = os.getenv("B2_APP_KEY")
    
    if not all([key_id, app_key, bucket_name]):
        print("Error: Missing B2 credentials or bucket name.")
        return

    # Create temp directory
    temp_dir = "/tmp/chunked_data"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        # Initialize B2 API
        print("Initializing B2 API...")
        info = b2.InMemoryAccountInfo()
        b2_api = b2.B2Api(info)
        
        # Authorize account
        print("Authorizing with B2...")
        b2_api.authorize_account("production", key_id, app_key)
        
        # Get the bucket
        print(f"Getting bucket: {bucket_name}")
        bucket = b2_api.get_bucket_by_name(bucket_name)
        
        # Estimate row size and calculate chunk size
        # First, sample a small portion of the DataFrame to estimate size
        sample_size = min(10000, df.height)
        sample_df = df.slice(0, sample_size)
        
        # Write sample to disk to check size
        sample_path = f"{temp_dir}/sample.parquet"
        sample_df.write_parquet(sample_path, compression="snappy")
        
        # Get sample file size in bytes
        sample_file_size = os.path.getsize(sample_path)
        
        # Calculate estimated size per row
        bytes_per_row = sample_file_size / sample_size
        
        # Calculate target size in bytes (with 10% margin to be safe)
        target_size_bytes = target_size_gb * 1024 * 1024 * 1024 * 0.9
        
        # Calculate rows per chunk
        rows_per_chunk = int(target_size_bytes / bytes_per_row)
        
        # Calculate number of chunks
        total_rows = df.height
        num_chunks = math.ceil(total_rows / rows_per_chunk)
        
        print(f"Splitting DataFrame with {total_rows} rows into {num_chunks} chunks of ~{rows_per_chunk} rows each")
        
        # Create and upload chunks
        for i in range(num_chunks):
            start_idx = i * rows_per_chunk
            end_idx = min((i + 1) * rows_per_chunk, total_rows)
            
            # Using Polars slice method
            chunk_df = df.slice(start_idx, end_idx - start_idx)
            chunk_path = f"{temp_dir}/chunk_{i:04d}.parquet"
            
            # Write chunk to local file
            print(f"Writing chunk {i+1}/{num_chunks} to local file...")
            chunk_df.write_parquet(chunk_path, compression="snappy")
            
            # Get file size before upload
            chunk_size_mb = os.path.getsize(chunk_path) / (1024 * 1024)
            print(f"Chunk {i+1}/{num_chunks} size: {chunk_size_mb:.2f} MB")
            
            # Upload file to B2
            b2_key = f"{prefix}/chunk_{i:04d}.parquet"
            print(f"Uploading chunk {i+1}/{num_chunks} to {bucket_name}/{b2_key}...")
            
            try:
                with open(chunk_path, 'rb') as file:
                    # Upload file data to B2
                    file_info = {}  # Optional metadata
                    bucket.upload_local_file(
                        local_file=chunk_path,
                        file_name=b2_key,
                        file_infos=file_info
                    )
                print(f"Successfully uploaded chunk {i+1}/{num_chunks}")
            except Exception as e:
                print(f"Error uploading chunk {i+1}/{num_chunks}: {str(e)}")
                print("Continuing with next chunk...")
                continue
            
            # Clean up local file to save space
            os.remove(chunk_path)
        
        # Clean up sample file
        os.remove(sample_path)
        
        # Create and upload metadata file with information about the chunks
        metadata = {
            "num_chunks": num_chunks,
            "total_rows": total_rows,
            "rows_per_chunk": rows_per_chunk,
            "compression": "snappy",
            "format_version": "1.0"
        }
        
        import json
        metadata_path = f"{temp_dir}/metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)
        
        try:
            print(f"Uploading metadata to {bucket_name}/{prefix}/metadata.json...")
            bucket.upload_local_file(
                local_file=metadata_path,
                file_name=f"{prefix}/metadata.json"
            )
            print("Successfully uploaded metadata")
        except Exception as e:
            print(f"Error uploading metadata: {str(e)}")
        
        os.remove(metadata_path)
        
        print("Chunking and upload process complete")
        
    except Exception as e:
        print(f"Error in chunking process: {str(e)}")


# Main script execution
if __name__ == "__main__":
    # Get bucket name from environment or allow override
    bucket_name = os.getenv("B2_BUCKET_NAME")
    
    # Check if we have required env vars before proceeding
    if not all([os.getenv("B2_KEY_ID"), os.getenv("B2_APP_KEY")]):
        print("ERROR: Missing required B2 credentials. Please check your .env file.")
        sys.exit(1)
    
    if not bucket_name:
        print("WARNING: B2_BUCKET_NAME not set in environment variables.")
        bucket_name = input("Please enter the B2 bucket name: ")
    
    try:
        with blackswan_sql_conn("crc_bloomberg_data") as conn:
            # Define the SQL query to fetch data
            sql_query = """
            SELECT * FROM RD_Equities_Hist
            """
            
            print("Reading data from SQL database...")
            df = pl.read_database(query=sql_query, connection=conn)
            print(f"Successfully read {df.height} rows from database")
            
            # Run the upload function
            saved_chunked_parquet_b2(
                df,
                bucket_name=bucket_name,
                prefix="data/chunked",
                target_size_gb=1
            )
            
    except Exception as e:
        print(f"Error during SQL connection or data processing: {str(e)}")