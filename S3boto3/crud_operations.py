import boto3
import pandas as pd
import logger
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
load_dotenv()


log = logger.init_logger()

s3 = boto3.client(service_name = 's3',
                    region_name = 'ap-southeast-2',
                    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
                    )

# creating a bucket
def create_bucket():
    # Bucket names must be between 3 and 63 characters long.
    # Bucket names can consist only of lowercase letters, numbers, hyphens (-), and periods (.).
    # Bucket names must start and end with a lowercase letter or number.
    # Bucket names cannot contain underscores (_) or uppercase letters.
    # Bucket names cannot be formatted as IP addresses (e.g., 192.168.5.4).
    # Bucket names cannot be in a format that resembles an IP address (e.g., 192.168.5.4).
    bucket_name = 'sus-bucket-yt'
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'ap-southeast-2'
            })
    log.info('Bucket created')

# list all buckets
def list_all_buckets():
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(bucket['Name'])
    log.info('Buckets listed')

# delete a bucket
def delete_bucket():
    bucket_name = 'sus-bucket-yt'
    s3.delete_bucket(Bucket=bucket_name)
    log.info('Bucket deleted')

# upload a file in bucket
def upload_file():
    bucket_name = 'sushi-bucket'
    s3.upload_file(Filename='customers-100.csv', Bucket=bucket_name, Key='customers-100.csv')
    log.info('File uploaded')

# delete a file in bucket
def delete_file():
    bucket_name = 'sushi-bucket'
    s3.delete_object(Bucket=bucket_name, Key='customers-100.csv')
    log.info('File deleted')

# list all files in bucket
def list_all_objects():
    bucket_name = 'sushi-bucket'
    response = s3.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        for obj in response['Contents']:
            log.info(obj['Key'])
    else:
        log.info("Bucket is empty.")

# get file from bucket
def get_file():
    bucket_name = 'sushi-bucket'
    object_key = 'customers-100.csv'
    
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            log.error(f"The specified key '{object_key}' does not exist in the bucket '{bucket_name}'.")
        else:
            log.error(f"An error occurred while getting the object: {e}")
    else:
        file_content = pd.read_csv(response['Body'])
        print(file_content.head(5))
        log.info('File fetched')

# download a file from bucket
def download_file():
    bucket_name = 'sushi-bucket'
    object_key = 'customers-100.csv'
    local_file_path = 'D:/customers.csv'
    try:
        s3.download_file(Bucket=bucket_name, Key=object_key, Filename=local_file_path)
        log.info(f"File '{object_key}' downloaded to '{local_file_path}' successfully.")
    except Exception as e:
        log.error(f"An error occurred while downloading the file: {e}")


if __name__ == "__main__":
    # create_bucket()
    # list_all_buckets()
    # delete_bucket()
    # upload_file()
    # delete_file()
    # list_all_objects()
    get_file()
    # download_file()
