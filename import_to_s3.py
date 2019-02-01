# compatible with python2.7
# This script will import directory structure to s3 bucket. Directory
# structure inside `path` will be imported to s3 bucket root.
# please update placeholders like PATH_OF_DATA_DIRECTORY, ACCESS_KEY and SECRET_ACCESS_KEY
# This is fastest way to import millions of small files to s3 bucket.

import boto3
import os
import dill
from pathos.multiprocessing import Pool,cpu_count
from contextlib import closing

path = 'PATH_OF_DATA_DIRECTORY'

session = boto3.Session(
    aws_access_key_id='ACCESS_KEY',
    aws_secret_access_key='SECRET_ACCESS_KEY'
)
s3 = session.resource('s3')
bucket = s3.Bucket('MY_BUCKET_NAME')
parallel_worker = 20

def call_execute(full_path):
    with open(full_path, 'rb') as data:
        bucket.put_object(Key=full_path[len(path)+1:], Body=data)

def upload_files(path):
    for subdir, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(subdir, file)

if __name__ == "__main__":
    #workers = cpu_count()
    workers = parallel_worker 
    with closing(Pool(processes=workers)) as pool:
        pool.map(call_execute , upload_files(path))
        pool.terminate()

