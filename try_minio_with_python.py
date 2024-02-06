import traceback
import boto3
from botocore.exceptions import NoCredentialsError

# Set your MinIO credentials and endpoint
minio_access_key = 'minioaccesskey'
minio_secret_key = 'miniosecretkey'
minio_endpoint = 'http://minio-server:9000'
source_bucket_name = 'test-bucket-1'
destination_bucket_name = 'test-bucket-2'

# FUNCTIONS
# =========

# Create an S3 client
def get_s3_client():
    try:
        s3 = boto3.client(
            's3', 
            endpoint_url=minio_endpoint, 
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key
        )
        return s3 
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Exception occurred while creating S3 client!\n{e}")
        traceback.print_exc()

# Create a bucket if not exists
def create_minio_bucket(bucket_name):
    # Create a bucket in MinIO
    try:
        s3 = get_s3_client()
        
        # Check if the bucket already exists
        response = s3.list_buckets()
        existing_buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        
        if bucket_name in existing_buckets:
            print(f"Bucket {bucket_name} already exists")
        else:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket {bucket_name} created successfully")
    except NoCredentialsError:
        print("Credentials not available")

# Check if an object exists in a bucket
def check_object_exists(source_bucket_name, object_name):
    # Check if an object exists in MinIO
    try:
        s3 = get_s3_client()
        response = s3.head_object(Bucket=source_bucket_name, Key=object_name)
        print(f"Object {object_name} exists in {source_bucket_name}")
    except NoCredentialsError:
        print("Credentials not available")
    except s3.exceptions.NoSuchKey:
        print(f"Object {object_name} does not exist in {source_bucket_name}")

# Check if an object from one bucket to another
def copy_object(
        source_bucket_name, 
        source_object_name, 
        destination_bucket_name,
        destination_object_name):
    # Copy an object in MinIO
    try:
        # Ensure the destination bucket exists
        create_minio_bucket(destination_bucket_name)
        
        s3 = get_s3_client()
        s3.copy_object(
            Bucket=destination_bucket_name,
            CopySource={'Bucket': source_bucket_name, 'Key': source_object_name},
            Key=destination_object_name
        )
        print(f"Object {source_bucket_name}/{source_object_name} copied to {destination_bucket_name}/{destination_object_name}")
    except NoCredentialsError:
        print("Credentials not available")

# Delete an object from bucket
def delete_object(bucket_name, object_name):
    # Delete an object from MinIO bucket
    try:
        s3 = get_s3_client()
        s3.delete_object(Bucket=bucket_name, Key=object_name)
        print(f"Object {object_name} deleted from {bucket_name}")
    except NoCredentialsError:
        print("Credentials not available")

# Upload data from local to S3 bucket
def upload_to_s3(file_path, bucket_name, object_name):
    # Upload a file to MinIO
    try:
        s3 = get_s3_client()
        # Create bucket if not exists
        create_minio_bucket(bucket_name)
        # Upload the file to s3 bucket
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded successfully to {bucket_name}/{object_name}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

# Download data from S3 bucket to local
def download_from_s3(source_bucket_name, object_name, local_file_path):
    # Download a file from MinIO
    try:
        s3 = get_s3_client()
        s3.download_file(source_bucket_name, object_name, local_file_path)
        print(f"File downloaded successfully to local [{local_file_path}]")
    except NoCredentialsError:
        print("Credentials not available")

# ACTION
# ======

# 1. Create MinIO bucket
create_minio_bucket(source_bucket_name)

# 2. Upload to MinIO
# Note: we copied our sample data (local) /opt/sample_data/sample_data.csv in Dockerfile.etl
upload_to_s3('/opt/sample_data/sample_data.csv', source_bucket_name, 'sample_data/sample_data.csv')

# 3. Check if an object exists (on source bucket)
check_object_exists(source_bucket_name, 'sample_data/sample_data.csv')

# 4. Copy object from one bucket to another
copy_object(source_bucket_name, 'sample_data/sample_data.csv', destination_bucket_name, 'sample_data/sample_data.csv')

# 5. Download from MinIO
download_from_s3(source_bucket_name, 'sample_data/sample_data.csv', '/opt/sample_data/sample_data1.csv')

# 6. Delete object from MinIO bucket
delete_object(source_bucket_name, 'sample_data/sample_data.csv')
