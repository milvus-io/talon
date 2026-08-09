"""Drive the Talon Azure gateway with Microsoft's Azure Storage SDK."""

import os
from datetime import datetime, timedelta, timezone

from azure.core import MatchConditions
from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import HttpResponseError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import (
    AccountSasPermissions,
    BlobPrefix,
    BlobSasPermissions,
    BlobServiceClient,
    ContentSettings,
    ResourceTypes,
    generate_account_sas,
    generate_blob_sas,
)


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

    mutation_name = "gateway/mutations/object.bin"
    mutation_blob = container.get_blob_client(mutation_name)
    mutation_blob.upload_blob(
        b"created-through-gateway",
        metadata={"owner": "talon"},
    )
    assert mutation_blob.download_blob().readall() == b"created-through-gateway"
    assert mutation_blob.download_blob().readall() == b"created-through-gateway"
    mutation_blob.upload_blob(b"overwritten-through-gateway", overwrite=True)
    assert mutation_blob.download_blob().readall() == b"overwritten-through-gateway"

    try:
        mutation_blob.upload_blob(b"must-not-replace", overwrite=False)
        raise AssertionError("conditional create unexpectedly replaced an existing blob")
    except (ResourceExistsError, HttpResponseError) as error:
        assert error.status_code in (409, 412)
    assert mutation_blob.download_blob().readall() == b"overwritten-through-gateway"

    mutation_blob.set_blob_metadata({"owner": "gateway", "stage": "e2e"})
    mutation_blob.set_http_headers(
        content_settings=ContentSettings(content_type="application/x-talon-e2e")
    )
    mutation_properties = mutation_blob.get_blob_properties()
    assert mutation_properties.metadata == {"owner": "gateway", "stage": "e2e"}
    assert mutation_properties.content_settings.content_type == "application/x-talon-e2e"

    copied_blob = container.get_blob_client("gateway/mutations/copied.bin")
    copy_result = copied_blob.start_copy_from_url(mutation_blob.url)
    assert copy_result["copy_status"] == "success"
    assert copied_blob.download_blob().readall() == b"overwritten-through-gateway"
    copied_blob.delete_blob()
    try:
        copied_blob.get_blob_properties()
        raise AssertionError("deleted copy unexpectedly remained visible")
    except ResourceNotFoundError:
        pass

    mutation_blob.delete_blob()
    try:
        mutation_blob.download_blob().readall()
        raise AssertionError("deleted blob unexpectedly remained visible")
    except ResourceNotFoundError:
        pass

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

    expiry = datetime.now(timezone.utc) + timedelta(minutes=5)
    blob_sas = generate_blob_sas(
        account_name=ACCOUNT,
        container_name=container_name,
        blob_name=object_name,
        account_key=ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=expiry,
        protocol="https,http",
    )
    sas_blob = BlobServiceClient(
        account_url=f"{gateway_endpoint}/{ACCOUNT}", credential=blob_sas
    ).get_blob_client(container_name, object_name)
    assert sas_blob.download_blob().readall() == expected_object()

    account_sas = generate_account_sas(
        account_name=ACCOUNT,
        account_key=ACCOUNT_KEY,
        resource_types=ResourceTypes(service=True, container=True, object=True),
        permission=AccountSasPermissions(read=True, list=True),
        expiry=expiry,
        protocol="https,http",
    )
    sas_service = BlobServiceClient(
        account_url=f"{gateway_endpoint}/{ACCOUNT}", credential=account_sas
    )
    assert (
        sas_service.get_blob_client(container_name, object_name)
        .download_blob()
        .readall()
        == expected_object()
    )
    assert any(
        item.name == "gateway/list/a space.txt"
        for item in sas_service.get_container_client(container_name).list_blobs(
            name_starts_with="gateway/list/"
        )
    )

    print("Azure SDK gateway conformance passed")


if __name__ == "__main__":
    main()
