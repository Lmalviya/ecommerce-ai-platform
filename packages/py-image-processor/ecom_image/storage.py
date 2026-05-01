import boto3
from .exceptions import StorageUploadError, DownloadError

class ImageStorage:
    """
    A generic S3-compatible storage client.
    Can be used for both Source and Destination buckets.
    """
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        self.endpoint = endpoint if endpoint.startswith("http") else f"http://{endpoint}"
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket

        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def upload(self, data: bytes, path: str, content_type: str = "image/jpeg") -> str:
        """Uploads bytes to the configured bucket."""
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=data,
                ContentType=content_type
            )
            return f"{self.endpoint}/{self.bucket}/{path}"
        except Exception as e:
            raise StorageUploadError(f"Upload failed to {self.bucket}/{path}: {str(e)}")

    def download(self, path: str) -> bytes:
        """Downloads bytes from the configured bucket."""
        try:
            # Handle s3:// prefix if passed
            clean_path = path.replace(f"s3://{self.bucket}/", "").replace("s3://", "")
            
            response = self.client.get_object(Bucket=self.bucket, Key=clean_path)
            return response['Body'].read()
        except Exception as e:
            raise DownloadError(f"Download failed from {self.bucket}/{path}: {str(e)}")
