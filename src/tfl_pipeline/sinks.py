"""Where the consumer writes files: a local folder or an S3 bucket."""

from pathlib import Path

import boto3


class LocalSink:
    """A sink is a place where the consumer writes files."""

    def __init__(self, root: str):
        self.root = Path(root)

    def write(self, key: str, data: bytes) -> str:
        """Write content to a file in the sink."""
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path)


class S3Sink:
    """A sink that writes files to an S3 bucket."""

    def __init__(self, bucket: str, prefix: str = ""):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client("s3")

    def write(self, key: str, data: bytes) -> str:
        """Write content to a file in the S3 bucket."""
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        self.client.put_object(Bucket=self.bucket, Key=full_key, Body=data)
        return f"s3://{self.bucket}/{full_key}"


def make_sink(uri: str) -> LocalSink | S3Sink:
    """Create a sink from a URI. Supported schemes: file://, s3://."""
    if uri.startswith("file://"):
        return LocalSink(uri[len("file://") :])
    elif uri.startswith("s3://"):
        parts = uri[len("s3://") :].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return S3Sink(bucket, prefix)

    return LocalSink(uri)
