import boto3
from botocore.client import Config
import os
from PIL import Image
import multiprocessing

Bucket = "bucket name"
prefix = "prefix of input folder"
oprefix = "prefix of output folder"

def s3_client():
    s3 = boto3.client('s3',
        endpoint_url=None,
        aws_access_key_id="access key",
        aws_secret_access_key="secret key",
        region_name="us-east-2",
        config=Config(signature_version='s3v4')
    )
    return s3

def list_buckets(client):
    resp = client.list_buckets()
    buckets = [bucket['Name'] for bucket in resp['Buckets']]
    return buckets

def list_objects(client):
    res = client.list_objects_v2(
        Bucket=Bucket,
        Prefix=prefix
    )
    return res['Contents']

def download_object(client, key, filename):
    client.download_file(Bucket,
        key,
        filename
    )

def compress_image(filename):
    image = Image.open(filename)
    image.save("min-"+filename,quality=40,optimize=True)

def upload_file(client, key, filename):
    new_key = key.replace(prefix, oprefix, 1)
    client.upload_file(
        "min-"+filename,
        Bucket,
        new_key
    )

def remove_file(client, key, filename):
    os.remove(filename)
    os.remove("min-"+filename)
    client.delete_object(Bucket=Bucket, Key=key)

def process_files(key):
    client = s3_client()
    path, filename = os.path.split(key)
    try:
        download_object(client, key, filename)
        compress_image(filename)
        upload_file(client, key, filename)
        remove_file(client, key, filename)
    except Exception as e:
        print(e)
        print("Error for the file: "+key)

def main():
    client = s3_client()
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=Bucket, Prefix=prefix)
    files = []
    for page in pages:
        for key in page['Contents']:
            if key['Size'] > 0:
                print(key['Key'])
                files.append(key['Key'])
    proc = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(proc)
    pool.map_async(process_files, files)
    pool.close()
    pool.join()

if __name__ == '__main__':
    main()
