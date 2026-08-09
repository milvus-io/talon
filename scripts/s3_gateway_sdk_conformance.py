"""Drive the Talon S3 gateway with boto3 and the MinIO Python SDK."""

import os
import urllib.request
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from minio import Minio


def expected_object() -> bytes:
    return bytes(index % 251 for index in range(4096))


def read_minio(response) -> bytes:
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def main() -> None:
    gateway_endpoint = os.environ["TALON_S3_GATEWAY_ENDPOINT"].rstrip("/")
    origin_endpoint = os.environ["TALON_S3_TEST_ENDPOINT"].rstrip("/")
    bucket = os.environ["TALON_S3_TEST_BUCKET"]
    object_name = os.environ["TALON_S3_TEST_KEY"]
    region = os.environ.get("TALON_S3_TEST_REGION", "us-east-1")
    access_key = os.environ["AWS_ACCESS_KEY_ID"]
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    config = Config(
        region_name=region,
        signature_version="s3v4",
        retries={"max_attempts": 0},
        s3={"addressing_style": "path"},
    )

    origin = boto3.client(
        "s3",
        endpoint_url=origin_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=config,
    )
    origin.put_object(Bucket=bucket, Key="gateway/list/a space.txt", Body=b"a")
    origin.put_object(Bucket=bucket, Key="gateway/list/child/b.txt", Body=b"b")

    s3 = boto3.client(
        "s3",
        endpoint_url=gateway_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=config,
    )
    properties = s3.head_object(Bucket=bucket, Key=object_name)
    etag = properties["ETag"]
    assert properties["ContentLength"] == 4096
    assert s3.get_object(Bucket=bucket, Key=object_name)["Body"].read() == expected_object()
    assert s3.get_object(Bucket=bucket, Key=object_name)["Body"].read() == expected_object()

    for start, end in [(0, 1023), (7, 1030), (4000, 4095)]:
        body = s3.get_object(
            Bucket=bucket, Key=object_name, Range=f"bytes={start}-{end}"
        )["Body"].read()
        assert body == expected_object()[start : end + 1]

    s3.head_object(Bucket=bucket, Key=object_name, IfMatch=etag)
    try:
        s3.get_object(Bucket=bucket, Key=object_name, IfMatch='"not-the-etag"')
        raise AssertionError("stale ETag unexpectedly succeeded")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 412
        assert error.response["Error"]["Code"] == "PreconditionFailed"

    try:
        s3.get_object(Bucket=bucket, Key="e2e/missing.bin")
        raise AssertionError("missing object unexpectedly succeeded")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404
        assert error.response["Error"]["Code"] == "NoSuchKey"

    assert (
        s3.get_object(Bucket=bucket, Key="gateway/list/a space.txt")["Body"].read()
        == b"a"
    )
    first = s3.list_objects_v2(Bucket=bucket, Prefix="gateway/list/", MaxKeys=1)
    assert first["IsTruncated"]
    second = s3.list_objects_v2(
        Bucket=bucket,
        Prefix="gateway/list/",
        ContinuationToken=first["NextContinuationToken"],
    )
    names = {item["Key"] for item in first.get("Contents", []) + second.get("Contents", [])}
    assert "gateway/list/a space.txt" in names
    walked = s3.list_objects_v2(
        Bucket=bucket, Prefix="gateway/list/", Delimiter="/"
    )
    assert any(item["Key"] == "gateway/list/a space.txt" for item in walked["Contents"])
    assert any(
        item["Prefix"] == "gateway/list/child/"
        for item in walked["CommonPrefixes"]
    )

    presigned = s3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": object_name}, ExpiresIn=60
    )
    with urllib.request.urlopen(presigned) as response:
        assert response.read() == expected_object()

    parsed = urlparse(gateway_endpoint)
    minio = Minio(
        parsed.netloc,
        access_key=access_key,
        secret_key=secret_key,
        secure=parsed.scheme == "https",
        region=region,
    )
    assert minio.stat_object(bucket, object_name).size == 4096
    assert read_minio(minio.get_object(bucket, object_name)) == expected_object()
    assert read_minio(minio.get_object(bucket, object_name, offset=7, length=1024)) == expected_object()[7:1031]
    minio_names = {
        item.object_name
        for item in minio.list_objects(bucket, prefix="gateway/list/", recursive=True)
    }
    assert "gateway/list/a space.txt" in minio_names
    assert "gateway/list/child/b.txt" in minio_names

    print("S3 SDK gateway conformance passed")


if __name__ == "__main__":
    main()
