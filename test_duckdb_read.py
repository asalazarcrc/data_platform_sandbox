import os
import json
import tempfile
import polars as pl
import duckdb
import b2sdk.v2 as b2
from dotenv import load_dotenv, find_dotenv
import time

class B2DuckDBConnector:
    """
    Connect B2 bucket data directly to DuckDB for efficient querying without downloading all files.
    """
    
    def __init__(self, bucket_name, prefix):
        """Initialize the connector with bucket and prefix information."""
        # Load environment variables
        load_dotenv(find_dotenv())
        
        # Get B2 credentials from environment
        self.key_id = os.getenv("B2_KEY_ID")
        self.app_key = os.getenv("B2_APP_KEY")
        self.bucket_name = bucket_name
        self.prefix = prefix
        
        if not all([self.key_id, self.app_key, self.bucket_name]):
            raise ValueError("Missing B2 credentials or bucket name")
        
        # Initialize B2 connection
        self._init_b2_connection()
        
        # Initialize DuckDB connection
        self.conn = duckdb.connect(database=':memory:')
        
        # Load metadata to understand file structure
        self.metadata = self._load_metadata()
        
        # Initialize file list and temp directory
        self.chunk_paths = []
        self.temp_dir = tempfile.mkdtemp()
        print(f"Created temporary directory: {self.temp_dir}")
        
    def _init_b2_connection(self):
        """Initialize the B2 SDK connection."""
        print("Initializing B2 API...")
        self.info = b2.InMemoryAccountInfo()
        self.b2_api = b2.B2Api(self.info)
        
        # Authorize account
        print("Authorizing with B2...")
        self.b2_api.authorize_account("production", self.key_id, self.app_key)
        
        # Get the bucket
        print(f"Getting bucket: {self.bucket_name}")
        self.bucket = self.b2_api.get_bucket_by_name(self.bucket_name)
    
    def _load_metadata(self):
        """Load metadata file to understand chunk structure."""
        metadata_key = f"{self.prefix}/metadata.json"
        
        try:
            # Download metadata to memory
            download_dest = b2.DownloadDestBytes()
            self.bucket.download_file_by_name(metadata_key, download_dest)
            metadata_content = download_dest.get_bytes_written()
            
            # Parse metadata JSON
            metadata = json.loads(metadata_content.decode('utf-8'))
            
            print(f"Loaded metadata: {len(metadata)} entries")
            print(f"Total rows: {metadata.get('total_rows', 'unknown')}")
            print(f"Number of chunks: {metadata.get('num_chunks', 'unknown')}")
            
            return metadata
        except Exception as e:
            print(f"Error loading metadata: {str(e)}")
            print("Will try to determine structure from available files")
            
            # List files to determine structure if metadata isn't available
            file_versions = list(self.bucket.list_file_versions(prefix=self.prefix))
            chunk_files = [file_info.file_name for file_info, _ in file_versions 
                          if file_info.file_name.endswith('.parquet')]
            
            return {
                "num_chunks": len(chunk_files),
                "chunk_files": chunk_files,
                "total_rows": "unknown"
            }
    
    def _download_chunks(self, limit=None):
        """Download parquet chunks to temporary directory."""
        num_chunks = self.metadata.get('num_chunks', 0)
        if limit is not None:
            num_chunks = min(num_chunks, limit)
            
        print(f"Downloading {num_chunks} chunks to temporary storage...")
        
        self.chunk_paths = []
        for i in range(num_chunks):
            chunk_key = f"{self.prefix}/chunk_{i:04d}.parquet"
            chunk_path = os.path.join(self.temp_dir, f"chunk_{i:04d}.parquet")
            
            print(f"Downloading chunk {i+1}/{num_chunks}...")
            download_dest = b2.DownloadDestLocalFile(chunk_path)
            self.bucket.download_file_by_name(chunk_key, download_dest)
            
            self.chunk_paths.append(chunk_path)
    
    def create_unified_view(self, view_name="b2_data"):
        """Create a unified view in DuckDB that represents all the data."""
        if not self.chunk_paths:
            # Download chunks if not already done
            self._download_chunks()
        
        if not self.chunk_paths:
            raise ValueError("No data chunks available")
        
        print(f"Creating unified view '{view_name}' from {len(self.chunk_paths)} parquet files...")
        
        # Create a unified view over all parquet files
        paths_str = ", ".join([f"'{path}'" for path in self.chunk_paths])
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT * FROM parquet_scan([{paths_str}]);
        """
        
        self.conn.execute(create_view_sql)
        print(f"Successfully created view '{view_name}'")
        
        return view_name
    
    def execute_query(self, sql_query, view_name="b2_data"):
        """Execute a SQL query on the unified view."""
        # Make sure the view exists
        if view_name not in self.list_views():
            self.create_unified_view(view_name)
        
        # Replace table name in query if needed
        if "FROM data_table" in sql_query:
            sql_query = sql_query.replace("FROM data_table", f"FROM {view_name}")
        
        # Execute the query
        print(f"Executing query: {sql_query}")
        result = self.conn.execute(sql_query)
        
        # Convert to Polars
        return pl.from_arrow(result.arrow())
    
    def list_views(self):
        """List all views in the DuckDB connection."""
        result = self.conn.execute("SHOW VIEWS;")
        views = [row[0] for row in result.fetchall()]
        return views
    
    def get_schema(self, view_name="b2_data"):
        """Get the schema of the view."""
        if view_name not in self.list_views():
            self.create_unified_view(view_name)
            
        result = self.conn.execute(f"DESCRIBE {view_name};")
        schema = result.fetchall()
        return schema
    
    def get_sample(self, view_name="b2_data", limit=10):
        """Get a sample of rows from the view."""
        if view_name not in self.list_views():
            self.create_unified_view(view_name)
            
        result = self.conn.execute(f"SELECT * FROM {view_name} LIMIT {limit};")
        return pl.from_arrow(result.arrow())
    
    def create_optimized_table(self, table_name="optimized_data", view_name="b2_data"):
        """Create an optimized table from the view for better query performance."""
        if view_name not in self.list_views():
            self.create_unified_view(view_name)
            
        print(f"Creating optimized table '{table_name}'...")
        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {view_name};")
        print(f"Created optimized table '{table_name}' with {self.conn.execute(f'SELECT COUNT(*) FROM {table_name}').fetchone()[0]} rows")
        
        return table_name
    
    def close(self):
        """Clean up resources."""
        # Close DuckDB connection
        self.conn.close()
        
        # Clean up temporary files
        print("Cleaning up temporary files...")
        for path in self.chunk_paths:
            if os.path.exists(path):
                os.remove(path)
        
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)
            print(f"Removed temporary directory: {self.temp_dir}")


if __name__ == "__main__":
    # Get bucket name from environment or user input
    bucket_name = os.getenv("B2_BUCKET_NAME")
    
    if not bucket_name:
        bucket_name = input("Please enter the B2 bucket name: ")
    
    # Path prefix where the data is stored
    prefix = "data/chunked"
    
    try:
        # Initialize connector
        connector = B2DuckDBConnector(bucket_name, prefix)
        
        # Create unified view
        view_name = connector.create_unified_view()
        
        # Get schema
        print("\nSchema:")
        schema = connector.get_schema()
        for column in schema:
            print(f"  {column[0]}: {column[1]}")
        
        # Get sample data
        print("\nSample data:")
        sample = connector.get_sample(limit=5)
        print(sample)
        
        # Example query 1: Count rows
        print("\nCounting rows...")
        result = connector.execute_query(f"SELECT COUNT(*) AS row_count FROM {view_name}")
        print(result)
        
        # Example query 2: Find min/max/avg of a numeric column
        # First, find a numeric column from the schema
        numeric_cols = [col[0] for col in schema if 'int' in col[1].lower() or 'float' in col[1].lower() or 'double' in col[1].lower()]
        
        if numeric_cols:
            numeric_col = numeric_cols[0]
            print(f"\nAnalyzing numeric column: {numeric_col}")
            result = connector.execute_query(f"""
            SELECT 
                MIN({numeric_col}) AS min_value,
                MAX({numeric_col}) AS max_value,
                AVG({numeric_col}) AS avg_value
            FROM {view_name}
            """)
            print(result)
        
        # Example query 3: Group by a categorical column
        # First, find a categorical column
        categorical_cols = [col[0] for col in schema if 'varchar' in col[1].lower() or 'string' in col[1].lower()]
        
        if categorical_cols:
            cat_col = categorical_cols[0]
            print(f"\nGroup by categorical column: {cat_col}")
            result = connector.execute_query(f"""
            SELECT 
                {cat_col}, 
                COUNT(*) AS count
            FROM {view_name}
            GROUP BY {cat_col}
            ORDER BY count DESC
            LIMIT 10
            """)
            print(result)
        
        # Optional: Create optimized table for better performance
        if input("\nCreate optimized table for better performance? (y/n): ").lower() == 'y':
            table_name = connector.create_optimized_table()
            
            # Example query on optimized table
            print("\nRunning query on optimized table...")
            start_time = time.time()
            result = connector.execute_query(f"SELECT COUNT(*) FROM {table_name}")
            end_time = time.time()
            print(f"Query took {end_time - start_time:.4f} seconds")
            print(result)
        
        # Clean up
        connector.close()
        
    except Exception as e:
        print(f"Error: {str(e)}")