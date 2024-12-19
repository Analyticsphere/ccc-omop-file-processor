from google.cloud import storage

def list_gcs_files(bucket_name: str, folder_prefix: str) -> list[str]:
    """Lists all files within a specific folder in a GCS bucket."""
    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        
        # Print available buckets for debugging
        print("Available buckets:")
        for bucket in storage_client.list_buckets():
            print(f"- {bucket.name}")
            
        # Get the bucket
        print(f"\nAttempting to access bucket: {bucket_name}")
        bucket = storage_client.bucket(bucket_name)
        
        # Verify bucket exists
        if not bucket.exists():
            raise Exception(f"Bucket {bucket_name} does not exist")
            
        # Rest of your function...
        if not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        print(f"Searching with prefix: {folder_prefix}")
        blobs = bucket.list_blobs(prefix=folder_prefix)
        
        files = [blob.name for blob in blobs 
                if blob.name != folder_prefix and not blob.name.endswith('/')]
        
        return files
    
    except Exception as e:
        raise Exception(f"Error listing files in GCS: {str(e)}")