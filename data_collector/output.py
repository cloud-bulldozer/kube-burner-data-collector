import os
import csv
import boto3
import logging
import tempfile

logger = logging.getLogger(__name__)

def upload_csv_to_s3(chunk_rows: list, fieldnames: list, bucket: str, foldername: str, filename: str):
    """
    Uploads a CSV file to Amazon S3.
    
    Args:
        chunk_rows (list): List of dictionaries representing CSV rows to write.
        fieldnames (list): List of field names for the CSV header.
        bucket (str): Name of the S3 bucket to upload to.
        foldername (str): S3 folder/prefix path where the file will be stored.
        filename (str): Name of the file to create in S3.
    """
    tmp = tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False)
    try:
        writer = csv.DictWriter(tmp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk_rows)
        tmp.flush()

        # Upload to S3
        s3 = boto3.client("s3")
        s3_key = f"{foldername.rstrip('/')}/{filename}"
        s3.upload_file(tmp.name, bucket, s3_key)
        logger.info(f"✅ Uploaded chunk to s3://{bucket}/{s3_key}")
    finally:
        tmp.close()
        os.remove(tmp.name)
        logger.info(f"🧹 Temporary file {tmp.name} deleted")

def write_to_file(chunk_rows: list, fieldnames: list, filename: str):
    """
    Writes csv into file
    
    Takes a list of rows and saves it as a CSV file to the local filesystem.
    
    Args:
        chunk_rows (list): The list of rows to write to file.
        fieldnames (list): The list of field names to write to file.
        filename (str): Path and name of the output CSV file.
    """
    with open(filename, "w") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk_rows)
    logger.info(f"✅ Output written in file {filename}")