import boto3
from botocore.config import Config
from app.config import settings
from app.logger import logger


class S3Client:
    def __init__(self):
        logger.info("Initializing S3 client")
        self.client = boto3.client(
            "s3",
            aws_access_key_id=settings.s3.access_key_id,
            aws_secret_access_key=settings.s3.secret_access_key,
            region_name=settings.s3.region,
            endpoint_url=settings.s3.endpoint_url,
            config=Config(
                signature_version="s3v4",
                s3={
                    "addressing_style": "path",
                    "payload_signing_enabled": False,
                },
                # verify=False,  # Раскомментируй, если HTTPS с self-signed cert
            ),
        )
        self.bucket = settings.s3.bucket_name
        logger.info(f"S3 client initialized for bucket: {self.bucket}")

    def list_objects(self, prefix: str):
        """List objects with prefix, return sorted by LastModified ascending (oldest first)"""
        logger.info(f"Listing S3 objects with prefix: {prefix}")
        objects = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" in page:
                objects.extend(page["Contents"])
        # Sort by LastModified ascending (oldest first)
        objects.sort(
            key=lambda x: x["LastModified"], reverse=False
        )  # Изменено: reverse=False для ascending
        keys = [obj["Key"] for obj in objects]
        logger.info(f"Found {len(keys)} objects, sorted oldest first")
        return keys

    def get_object(self, key: str):
        """Download object as bytes"""
        logger.info(f"Downloading S3 object: {key}")
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        data = response["Body"].read()
        logger.info(f"Downloaded {key}, size: {len(data)} bytes")
        return data
