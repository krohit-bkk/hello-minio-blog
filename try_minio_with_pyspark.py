import os
import sys
sys.path.append('.')

# Reuse functions from our previous demo
from try_minio_with_python import *
# PySpark related imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lit

# Set your MinIO credentials and endpoint
minio_access_key = 'minioaccesskey'
minio_secret_key = 'miniosecretkey'
minio_endpoint = 'http://minio-server:9000'
minio_bucket = "test-bucket-1"
minio_sample_file_path = "sample/sample_data.csv"

# Function to get SparkSession object
def get_spark():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("MinIO PySpark Example") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Set the S3A configuration for MinIO
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", minio_access_key))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", minio_secret_key))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("ENDPOINT", minio_endpoint))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

    spark.sparkContext.setLogLevel("WARN")
    return spark 

# Create dataframe on S3 object
def read_csv_as_dataframe(path, schema):
  spark = get_spark()
  df = spark.read.csv(path, header=False, schema=schema)
  return df

# Write data to S3
def write_to_s3(df, path, mode):
  df.write.format("parquet").mode(mode).save(path)


bucket_name = "test-bucket-1"
object_orig_path = "sample/sample_data.csv"
object_target_path = "sample1/"

# Step-1: Loading data
# 1. Create MinIO bucket if not exists
create_minio_bucket(bucket_name)
# 2. Upload to MinIO
# Note: we copied our sample data (local) /opt/sample_data/sample_data.csv in Dockerfile.etl
upload_to_s3('/opt/sample_data/sample_data.csv', bucket_name, object_orig_path)

# Step-2: Read data as dataframe
# Schema for our sample_data.csv
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Reading the file as dataframe
path = f"s3a://{bucket_name}/{object_orig_path}"
df1 = read_csv_as_dataframe(path, schema)
df1.show(20, False)

# Step-3: Transform and write back to S3
# Transform the data
df2 = df1.withColumn("Flag", lit("df2"))
output_path = f"s3a://{bucket_name}/{object_target_path}"
df2.show(20, False)

# Write data to S3 bucket
write_to_s3(df2, output_path, "overwrite")

# Step-4: Read from where we wrote data above to confirm
# Read from the target location
df3 = get_spark().read.parquet(output_path)
df3.show(20, False)

# Step-5: Close SparkSession
get_spark().stop()
