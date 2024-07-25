from io import BytesIO
from os import urandom
from typing import Callable

import pytest
from s3lite import Client
from s3lite.auth import SignedClient
from s3lite.client import ClientConfig


@pytest.mark.asyncio
async def test_bucket_create(s3_client: Client):
    bucket = await s3_client.create_bucket("test_bucket")
    assert bucket.name == "test_bucket"


@pytest.mark.asyncio
async def test_list_buckets(s3_client: Client):
    bucket = await s3_client.create_bucket("test_bucket1")
    assert bucket is not None

    buckets = await s3_client.ls_buckets()
    assert len(buckets) == 1
    assert buckets[0].name == "test_bucket1"


@pytest.mark.asyncio
async def test_upload_singlepart(s3_client: Client):
    bucket = await s3_client.create_bucket("test_bucket2")
    assert bucket is not None

    file = BytesIO(urandom(1024 * 16))
    obj = await bucket.upload("test/test.bin", file)
    assert obj is not None


@pytest.mark.asyncio
async def test_list_objects(s3_client: Client):
    bucket = await s3_client.create_bucket("test_bucket3")
    assert bucket is not None

    for i in range(8):
        file = BytesIO(urandom(1024 * 16))
        obj = await bucket.upload(f"test/test_{i}.bin", file)
        assert obj is not None

    objects = await bucket.ls()
    assert len(objects) == 8
    assert {obj.name for obj in objects} == {f"test/test_{i}.bin" for i in range(8)}


@pytest.mark.asyncio
async def test_download(s3_client: Client):
    bucket = await s3_client.create_bucket("test_bucket4")
    assert bucket is not None

    file = BytesIO(urandom(1024 * 16))
    obj = await bucket.upload("test/test.bin", file)
    assert obj is not None

    downloaded = await obj.download(in_memory=True)
    assert isinstance(downloaded, BytesIO)
    assert downloaded.getvalue() == file.getvalue()

    downloaded = await obj.download(in_memory=True, offset=16, limit=1024 * 16 - 32 + 1)
    assert isinstance(downloaded, BytesIO)
    assert downloaded.getvalue() == file.getvalue()[16:-16]


@pytest.mark.asyncio
async def test_delete_objects(s3_client: Client):
    bucket = await s3_client.create_bucket("test_bucket5")
    assert bucket is not None

    objs = []
    for i in range(8):
        file = BytesIO(urandom(1024))
        obj = await bucket.upload(f"test/test_{i}.bin", file)
        objs.append(obj)
        assert obj is not None

    await objs[4].delete()
    await objs[6].delete()
    del objs[6]
    del objs[4]

    objects = await bucket.ls()
    assert len(objects) == 6
    assert {obj.name for obj in objects} == {obj.name for obj in objs}


@pytest.mark.asyncio
async def test_delete_bucket(s3_client: Client):
    await s3_client.ls_buckets()

    bucket1 = await s3_client.create_bucket("test_bucket6_1")
    assert bucket1 is not None
    bucket2 = await s3_client.create_bucket("test_bucket6_2")
    assert bucket2 is not None
    bucket3 = await s3_client.create_bucket("test_bucket6_3")
    assert bucket3 is not None

    buckets = await s3_client.ls_buckets()
    assert len(buckets) == 3

    await s3_client.delete_bucket(bucket2)

    buckets = await s3_client.ls_buckets()
    assert len(buckets) == 2
    assert {bucket.name for bucket in buckets} == {f"test_bucket6_{i}" for i in (1, 3)}


@pytest.mark.asyncio
async def test_upload_multipart(client_factory: Callable[..., SignedClient]):
    s3_client = Client(
        "valid_access",
        "access_key",
        "https://s3server.test",
        config=ClientConfig(multipart_threshold=1024 * 4),
        httpx_client=client_factory,
    )

    bucket = await s3_client.create_bucket("test_bucket7")
    assert bucket is not None

    file = BytesIO(urandom(1024 * 16))
    obj = await bucket.upload("test/test.bin", file)
    assert obj is not None

    downloaded = await obj.download(in_memory=True)
    assert isinstance(downloaded, BytesIO)
    assert downloaded.getvalue() == file.getvalue()
