"""Drive the Talon Azure gateway with Microsoft's Azure Storage SDK."""

import os

from azure.core import MatchConditions
from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.storage.blob import BlobPrefix, BlobServiceClient


ACCOUNT = "devstoreaccount1"
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)


def expected_object() -> bytes:
    return bytes(index % 251 for index in range(4096))


def main() -> None:
    gateway_endpoint = os.environ["TALON_AZURE_GATEWAY_ENDPOINT"].rstrip("/")
    container_name = os.environ["TALON_AZURE_TEST_CONTAINER"]
    object_name = os.environ["TALON_AZURE_TEST_KEY"]

    origin = BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )
    origin_container = origin.get_container_client(container_name)
    origin_container.upload_blob("gateway/list/a space.txt", b"a", overwrite=True)
    origin_container.upload_blob("gateway/list/child/b.txt", b"b", overwrite=True)

    gateway = BlobServiceClient(
        account_url=f"{gateway_endpoint}/{ACCOUNT}",
        credential=AzureNamedKeyCredential(ACCOUNT, ACCOUNT_KEY),
    )
    container = gateway.get_container_client(container_name)
    blob = container.get_blob_client(object_name)

    properties = blob.get_blob_properties()
    assert properties.size == 4096
    assert blob.download_blob().readall() == expected_object()
    assert blob.download_blob().readall() == expected_object()

    for offset, length in [(0, 1024), (7, 1024), (4000, 96)]:
        assert blob.download_blob(offset=offset, length=length).readall() == expected_object()[
            offset : offset + length
        ]

    blob.get_blob_properties(
        etag=properties.etag,
        match_condition=MatchConditions.IfNotModified,
    )
    try:
        blob.get_blob_properties(
            etag='"not-the-etag"',
            match_condition=MatchConditions.IfNotModified,
        )
        raise AssertionError("stale ETag unexpectedly succeeded")
    except HttpResponseError as error:
        assert error.status_code == 412

    try:
        container.get_blob_client("e2e/missing.bin").get_blob_properties()
        raise AssertionError("missing blob unexpectedly succeeded")
    except ResourceNotFoundError:
        pass

    assert (
        container.get_blob_client("gateway/list/a space.txt")
        .download_blob()
        .readall()
        == b"a"
    )

    pager = container.list_blobs(
        name_starts_with="gateway/list/", results_per_page=1
    ).by_page()
    first_page = list(next(pager))
    assert pager.continuation_token
    second_page = list(next(pager))
    names = {item.name for item in first_page + second_page}
    assert "gateway/list/a space.txt" in names

    walked = list(
        container.walk_blobs(name_starts_with="gateway/list/", delimiter="/")
    )
    assert any(item.name == "gateway/list/a space.txt" for item in walked)
    assert any(
        isinstance(item, BlobPrefix) and item.name == "gateway/list/child/"
        for item in walked
    )

    print("Azure SDK gateway conformance passed")


if __name__ == "__main__":
    main()
