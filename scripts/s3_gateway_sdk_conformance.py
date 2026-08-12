"""Drive the Talon S3 gateway with boto3 and the MinIO Python SDK."""

import base64
import hashlib
import io
import os
import urllib.error
import urllib.request
from urllib.parse import urlparse

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.client import Config
from botocore.credentials import Credentials
from botocore.exceptions import ClientError
from minio import Minio
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
from minio.error import S3Error


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
        request_checksum_calculation="when_required",
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
    v1_names = [
        item["Key"]
        for item in s3.list_objects(Bucket=bucket, Prefix="gateway/list/").get(
            "Contents", []
        )
    ]
    assert "gateway/list/a space.txt" in v1_names, v1_names
    assert "gateway/list/child/b.txt" in v1_names, v1_names
    first_page = s3.list_objects(Bucket=bucket, Prefix="gateway/list/", MaxKeys=1)
    assert first_page["IsTruncated"] is True
    # The marker deliberately needs no URL encoding: LocalStack compares raw
    # markers against URL-encoded keys under the encoding-type=url parameter
    # boto3 injects, so a marker containing a space never matches there. Real
    # S3 applies encoding-type to the response only. The space-keyed object is
    # still covered by the full-listing assertion above.
    second_page = s3.list_objects(
        Bucket=bucket,
        Prefix="gateway/list/",
        Marker="gateway/list/b",
    )
    second_names = [item["Key"] for item in second_page.get("Contents", [])]
    assert second_names == ["gateway/list/child/b.txt"], (
        f"V1 Marker pagination must resume after the marker key: {second_names}"
    )

    s3.head_bucket(Bucket=bucket)
    location = s3.get_bucket_location(Bucket=bucket)
    expected_location = None if region == "us-east-1" else region
    assert location.get("LocationConstraint") == expected_location, location
    try:
        s3.head_bucket(Bucket=f"{bucket}-missing")
        raise AssertionError("missing bucket unexpectedly exists")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404

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
    mutation_key = "gateway/mutations/object.bin"
    copy_key = "gateway/mutations/copied.bin"
    first_body = b"created-through-gateway"
    second_body = b"overwritten-through-gateway"
    checksum = base64.b64encode(hashlib.sha256(first_body).digest()).decode("ascii")
    created = s3.put_object(
        Bucket=bucket,
        Key=mutation_key,
        Body=first_body,
        Metadata={"owner": "sdk-conformance"},
        ChecksumSHA256=checksum,
        IfNoneMatch="*",
    )
    assert created["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3.get_object(Bucket=bucket, Key=mutation_key)["Body"].read() == first_body
    metadata = s3.head_object(Bucket=bucket, Key=mutation_key)
    assert metadata["Metadata"]["owner"] == "sdk-conformance"

    try:
        s3.put_object(
            Bucket=bucket,
            Key=mutation_key,
            Body=b"must-not-land",
            IfNoneMatch="*",
        )
        raise AssertionError("conditional overwrite unexpectedly succeeded")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 412

    s3.put_object(Bucket=bucket, Key=mutation_key, Body=second_body)
    assert s3.get_object(Bucket=bucket, Key=mutation_key)["Body"].read() == second_body

    # Streaming aws-chunked upload (STREAMING-UNSIGNED-PAYLOAD-TRAILER), the
    # AWS C++ SDK's default write framing. boto3 pre-hashes an in-memory body
    # into a header checksum, so the framing is built and SigV4-signed by hand
    # to exercise the gateway's decoder and checksum verifier with real signed
    # trailer traffic. Both sha256 and crc64nvme (the AWS SDK default and this
    # feature's motivating algorithm) are driven end to end.
    trailer_payload = b"streamed-through-the-aws-chunked-decoder-" * 64

    def crc64nvme(data):
        # CRC-64/NVME (reflected poly 0x9a6c9329ac4bc9b5), the AWS SDK default.
        crc = 0xFFFFFFFFFFFFFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                crc = (crc >> 1) ^ (0x9A6C9329AC4BC9B5 & -(crc & 1))
            crc &= 0xFFFFFFFFFFFFFFFF
        return crc ^ 0xFFFFFFFFFFFFFFFF

    def trailer_checksum(payload, algorithm):
        if algorithm == "sha256":
            return base64.b64encode(hashlib.sha256(payload).digest()).decode()
        if algorithm == "crc64nvme":
            return base64.b64encode(crc64nvme(payload).to_bytes(8, "big")).decode()
        raise ValueError(algorithm)

    def signed_trailer_put(key, payload, algorithm, checksum):
        framed = bytearray()
        framed += f"{len(payload):x}\r\n".encode() + payload + b"\r\n0\r\n"
        framed += f"x-amz-checksum-{algorithm}:{checksum}\r\n\r\n".encode()
        request = AWSRequest(
            method="PUT",
            url=f"{gateway_endpoint}/{bucket}/{key}",
            data=bytes(framed),
            headers={
                "host": urlparse(gateway_endpoint).netloc,
                "x-amz-content-sha256": "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
                "x-amz-decoded-content-length": str(len(payload)),
                "content-encoding": "aws-chunked",
                "x-amz-trailer": f"x-amz-checksum-{algorithm}",
                "content-length": str(len(framed)),
            },
        )
        SigV4Auth(Credentials(access_key, secret_key), "s3", region).add_auth(request)
        prepared = request.prepare()
        return urllib.request.Request(
            prepared.url, data=prepared.body, headers=dict(prepared.headers), method="PUT"
        )

    for algorithm in ("sha256", "crc64nvme"):
        key = f"gateway/mutations/aws-chunked-{algorithm}.bin"
        checksum = trailer_checksum(trailer_payload, algorithm)
        with urllib.request.urlopen(
            signed_trailer_put(key, trailer_payload, algorithm, checksum)
        ) as response:
            assert response.status == 200, (algorithm, response.status)
        assert (
            s3.get_object(Bucket=bucket, Key=key)["Body"].read() == trailer_payload
        ), f"{algorithm}: origin must store the decoded payload, not the framing"
        s3.delete_object(Bucket=bucket, Key=key)

    # A corrupt trailer checksum must be rejected at the gateway, never stored.
    corrupt_key = "gateway/mutations/aws-chunked-corrupt.bin"
    try:
        urllib.request.urlopen(
            signed_trailer_put(corrupt_key, trailer_payload, "sha256", "AAAA")
        )
        raise AssertionError("a wrong trailer checksum was accepted")
    except urllib.error.HTTPError as error:
        assert error.code == 400, error.code
    try:
        s3.head_object(Bucket=bucket, Key=corrupt_key)
        raise AssertionError("a checksum-mismatched object was stored")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    copied = s3.copy_object(
        Bucket=bucket,
        Key=copy_key,
        CopySource={"Bucket": bucket, "Key": mutation_key},
        Metadata={"copied": "true"},
        MetadataDirective="REPLACE",
    )
    assert copied["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3.get_object(Bucket=bucket, Key=copy_key)["Body"].read() == second_body
    assert s3.head_object(Bucket=bucket, Key=copy_key)["Metadata"]["copied"] == "true"

    s3.delete_object(Bucket=bucket, Key=mutation_key)
    s3.delete_object(Bucket=bucket, Key=mutation_key)
    s3.delete_object(Bucket=bucket, Key=copy_key)
    for deleted_key in (mutation_key, copy_key):
        try:
            s3.head_object(Bucket=bucket, Key=deleted_key)
            raise AssertionError("deleted object unexpectedly exists")
        except ClientError as error:
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    # Batch DeleteObjects: one POST ?delete removes several keys with per-key
    # results, and deleting a missing key still reports Deleted (S3 deletes
    # are idempotent).
    batch_keys = [f"gateway/mutations/batch-{index}.bin" for index in range(3)]
    for key in batch_keys:
        s3.put_object(Bucket=bucket, Key=key, Body=b"batch")
    batch = s3.delete_objects(
        Bucket=bucket,
        Delete={
            "Objects": [{"Key": key} for key in batch_keys]
            + [{"Key": "gateway/mutations/batch-missing.bin"}],
            "Quiet": False,
        },
    )
    assert not batch.get("Errors"), batch.get("Errors")
    deleted_keys = sorted(entry["Key"] for entry in batch.get("Deleted", []))
    assert deleted_keys == sorted(
        batch_keys + ["gateway/mutations/batch-missing.bin"]
    ), deleted_keys
    for key in batch_keys:
        try:
            s3.head_object(Bucket=bucket, Key=key)
            raise AssertionError("batch-deleted object unexpectedly exists")
        except ClientError as error:
            assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    # Quiet mode omits the Deleted echo but still deletes.
    quiet_key = "gateway/mutations/batch-quiet.bin"
    s3.put_object(Bucket=bucket, Key=quiet_key, Body=b"quiet")
    quiet = s3.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": quiet_key}], "Quiet": True}
    )
    assert not quiet.get("Errors"), quiet.get("Errors")
    assert not quiet.get("Deleted"), "quiet mode must not echo Deleted entries"
    try:
        s3.head_object(Bucket=bucket, Key=quiet_key)
        raise AssertionError("quiet batch delete left the object behind")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    multipart_key = "gateway/multipart/boto3.bin"
    multipart = s3.create_multipart_upload(
        Bucket=bucket, Key=multipart_key, Metadata={"owner": "boto3-multipart"}
    )
    upload_id = multipart["UploadId"]
    part_one = b"a" * (5 * 1024 * 1024)
    part_two = b"tail"
    uploaded_one = s3.upload_part(
        Bucket=bucket, Key=multipart_key, UploadId=upload_id,
        PartNumber=1, Body=part_one,
    )
    uploaded_two = s3.upload_part(
        Bucket=bucket, Key=multipart_key, UploadId=upload_id,
        PartNumber=2, Body=part_two,
    )
    listed_parts = s3.list_parts(Bucket=bucket, Key=multipart_key, UploadId=upload_id)
    assert [part["PartNumber"] for part in listed_parts["Parts"]] == [1, 2]
    completed = s3.complete_multipart_upload(
        Bucket=bucket, Key=multipart_key, UploadId=upload_id,
        MultipartUpload={"Parts": [
            {"PartNumber": 1, "ETag": uploaded_one["ETag"]},
            {"PartNumber": 2, "ETag": uploaded_two["ETag"]},
        ]},
    )
    assert completed["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3.get_object(Bucket=bucket, Key=multipart_key)["Body"].read() == part_one + part_two

    invalid_key = "gateway/multipart/invalid-order.bin"
    invalid = s3.create_multipart_upload(Bucket=bucket, Key=invalid_key)
    invalid_id = invalid["UploadId"]
    invalid_one = s3.upload_part(
        Bucket=bucket, Key=invalid_key, UploadId=invalid_id,
        PartNumber=1, Body=part_one,
    )
    invalid_two = s3.upload_part(
        Bucket=bucket, Key=invalid_key, UploadId=invalid_id,
        PartNumber=2, Body=part_two,
    )
    try:
        s3.complete_multipart_upload(
            Bucket=bucket, Key=invalid_key, UploadId=invalid_id,
            MultipartUpload={"Parts": [
                {"PartNumber": 2, "ETag": invalid_two["ETag"]},
                {"PartNumber": 1, "ETag": invalid_one["ETag"]},
            ]},
        )
        raise AssertionError("out-of-order multipart completion unexpectedly succeeded")
    except ClientError as error:
        assert error.response["ResponseMetadata"]["HTTPStatusCode"] >= 400
    s3.abort_multipart_upload(Bucket=bucket, Key=invalid_key, UploadId=invalid_id)

    copy_multipart_key = "gateway/multipart/copied.bin"
    copy_upload = s3.create_multipart_upload(Bucket=bucket, Key=copy_multipart_key)
    copy_upload_id = copy_upload["UploadId"]
    copied_part = s3.upload_part_copy(
        Bucket=bucket, Key=copy_multipart_key, UploadId=copy_upload_id,
        PartNumber=1, CopySource={"Bucket": bucket, "Key": multipart_key},
        CopySourceRange=f"bytes=0-{len(part_one) - 1}",
    )
    s3.complete_multipart_upload(
        Bucket=bucket, Key=copy_multipart_key, UploadId=copy_upload_id,
        MultipartUpload={"Parts": [{
            "PartNumber": 1,
            "ETag": copied_part["CopyPartResult"]["ETag"],
        }]},
    )
    assert s3.get_object(Bucket=bucket, Key=copy_multipart_key)["Body"].read() == part_one

    aborted_key = "gateway/multipart/aborted.bin"
    aborted = s3.create_multipart_upload(Bucket=bucket, Key=aborted_key)
    aborted_id = aborted["UploadId"]
    s3.upload_part(
        Bucket=bucket, Key=aborted_key, UploadId=aborted_id,
        PartNumber=1, Body=part_two,
    )
    s3.abort_multipart_upload(Bucket=bucket, Key=aborted_key, UploadId=aborted_id)
    try:
        s3.list_parts(Bucket=bucket, Key=aborted_key, UploadId=aborted_id)
        raise AssertionError("aborted multipart upload unexpectedly remained visible")
    except ClientError as error:
        assert error.response["Error"]["Code"] == "NoSuchUpload"
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
    assert minio.bucket_exists(bucket)
    assert not minio.bucket_exists(f"{bucket}-missing")
    minio_bootstrap = Minio(
        parsed.netloc,
        access_key=access_key,
        secret_key=secret_key,
        secure=parsed.scheme == "https",
    )
    assert minio_bootstrap.bucket_exists(bucket), (
        "a client without a configured region must resolve it through the "
        "gateway's location probe"
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

    minio_key = "gateway/mutations/minio.bin"
    minio_copy_key = "gateway/mutations/minio-copy.bin"
    minio_body = b"written-with-minio"
    minio.put_object(
        bucket,
        minio_key,
        io.BytesIO(minio_body),
        len(minio_body),
        metadata={"owner": "minio"},
    )
    assert read_minio(minio.get_object(bucket, minio_key)) == minio_body
    minio.copy_object(bucket, minio_copy_key, CopySource(bucket, minio_key))
    assert read_minio(minio.get_object(bucket, minio_copy_key)) == minio_body
    minio.remove_object(bucket, minio_key)
    minio.remove_object(bucket, minio_copy_key)

    # minio-go/minio-py remove_objects batches into POST ?delete — the call
    # milvus proxies issue for bulk cleanup.
    minio_batch_keys = [f"gateway/mutations/minio-batch-{index}.bin" for index in range(2)]
    for key in minio_batch_keys:
        minio.put_object(bucket, key, io.BytesIO(b"mb"), 2)
    batch_errors = list(
        minio.remove_objects(bucket, [DeleteObject(key) for key in minio_batch_keys])
    )
    assert not batch_errors, batch_errors
    for key in minio_batch_keys:
        try:
            minio.stat_object(bucket, key)
            raise AssertionError("minio batch delete left an object behind")
        except S3Error as error:
            assert error.code == "NoSuchKey", error.code

    minio_multipart_key = "gateway/multipart/minio.bin"
    minio_multipart_body = b"m" * (6 * 1024 * 1024)
    minio.put_object(
        bucket, minio_multipart_key, io.BytesIO(minio_multipart_body),
        len(minio_multipart_body), part_size=5 * 1024 * 1024,
    )
    assert read_minio(minio.get_object(bucket, minio_multipart_key)) == minio_multipart_body
    minio.remove_object(bucket, minio_multipart_key)
    for key in (multipart_key, copy_multipart_key):
        s3.delete_object(Bucket=bucket, Key=key)

    print("S3 SDK gateway conformance passed")


if __name__ == "__main__":
    main()
