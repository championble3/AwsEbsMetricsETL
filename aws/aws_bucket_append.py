import boto3
import pandas as pd 

s3 = boto3.client('s3', region_name='us-east-1')

bucket_name = 'mybuckettm1'
img_path = r"C:\Users\tomai\Pictures\Screenshots\Zrzut ekranu 2025-06-25 143052.png"
csv_path = r"C:\Users\tomai\Downloads\pingtest_data.csv"

def bucket_upload(path, bucket, key):
    try:
        s3.upload_file(path,bucket,key)
        print("File has been uploaded to S3 bucket.")
    except Exception as e:
        print(f"An error occurred: {e}")

bucket_upload(img_path, bucket_name, 'images/screenshot.png')
bucket_upload(csv_path, bucket_name, 'data/pingtest_data.csv')
