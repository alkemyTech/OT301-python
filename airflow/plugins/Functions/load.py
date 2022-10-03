import logging as log
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging


logging.basicConfig(level = logging.INFO,
                    format = " %(asctime)s - %(name)s - %(message)s",
                    datefmt='%Y-%m-%d',
                    encoding= "utf-8")

logger = logging.getLogger('')

def load(filename:str, key:str, bucket_name:str):
    """
    Function that is responsible for uploading the data to amazon s3
    """
    log.info(f'Uploading file to s3 {filename}')
    hook = S3Hook(aws_conn_id='aws_s3_bucket')
    log.info('Uploading file')
    hook.load_file(
        filename=filename,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )
    log.info('File uploaded to s3 successfully')