from collections import defaultdict
from io import BytesIO
from typing import Callable

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from s3lite import Client
from s3lite.auth import SignedClient

from pys3server import S3Server, BaseInterface, Bucket, S3Object, Part, BaseWriteStream, BaseReadStream, AccessDenied, \
    InvalidAccessKeyId


class InMemoryWriteStream(BaseWriteStream):
    def __init__(self, file: BytesIO):
        self._file = file

    async def write(self, content: bytes | None) -> None:
        if content is not None:
            self._file.write(content)


class InMemoryReadStream(BaseReadStream):
    def __init__(self, file: BytesIO, start_pos: int = 0, to_read: int = -1):
        self._file = file
        self._pos = start_pos
        self._to_read = len(file.getvalue()) if to_read < 0 else to_read

    async def read(self) -> bytes | None:
        self._file.seek(self._pos)
        data = self._file.read(min(self._to_read, 32 * 1024))
        self._pos += len(data)
        self._to_read -= len(data)

        return data if data else None

    async def supports_range(self) -> bool:
        return True

    async def total_size(self) -> int | None:
        return len(self._file.getvalue())


class SparseList(list):
    def __setitem__(self, index, value):
        missing = index - len(self) + 1
        if missing > 0:
            self.extend([None for _ in range(missing)])
        list.__setitem__(self, index, value)

    def __getitem__(self, index):
        try:
            return list.__getitem__(self, index)
        except IndexError:
            return None


class InMemoryInterface(BaseInterface):
    def __init__(self):
        self._buckets: dict[str, Bucket] = {}
        self._objects: dict[str, dict[str, BytesIO]] = defaultdict(dict)
        self._multipart_uploads: dict[str, dict[str, SparseList[BytesIO]]] = defaultdict(dict)

    async def access_key(self, key_id: str | None, object_: S3Object | Bucket | None) -> str | None:
        if key_id == "valid_access":
            return "access_key"
        if key_id == "valid_no_access":
            raise AccessDenied()
        if key_id == "invalid":
            raise InvalidAccessKeyId()

        bucket = object_.bucket if isinstance(object_, S3Object) else object_
        if bucket is None:
            raise AccessDenied()

        if bucket.name == "public":
            return None

        raise AccessDenied()

    async def create_bucket(self, key_id: str, bucket: str) -> Bucket:
        self._buckets[bucket] = Bucket(bucket)
        return self._buckets[bucket]

    async def list_buckets(self, key_id: str) -> list[Bucket]:
        return [Bucket(bucket_name) for bucket_name in self._buckets]

    async def list_bucket(self, key_id: str, bucket: Bucket) -> list[S3Object]:
        return [
            S3Object(bucket, object_name, len(file.getvalue()))
            for object_name, file in self._objects[bucket.name].items()
        ]

    async def read_object(self, key_id: str, object_: S3Object,
                          content_range: tuple[int, int] | None = None) -> BaseReadStream:
        file = self._objects[object_.bucket.name][object_.name]
        if content_range is not None:
            return InMemoryReadStream(file, content_range[0], content_range[1] - content_range[0])
        return InMemoryReadStream(file)

    async def write_object(self, key_id: str, bucket: Bucket, object_name: str, size: int) -> BaseWriteStream:
        self._objects[bucket.name][object_name] = file = BytesIO()
        return InMemoryWriteStream(file)

    async def create_multipart_upload(self, key_id: str, bucket: Bucket, object_name: str) -> S3Object:
        self._multipart_uploads[bucket.name][object_name] = SparseList()
        return S3Object(bucket, object_name, 0)

    async def write_object_multipart(self, object_: S3Object, part_id: int, size: int) -> BaseWriteStream:
        self._multipart_uploads[object_.bucket.name][object_.name][part_id - 1] = file = BytesIO()
        return InMemoryWriteStream(file)

    async def finish_multipart_upload(self, object_: S3Object, parts: list[Part]) -> None:
        self._objects[object_.bucket.name][object_.name] = file = BytesIO()
        for part in self._multipart_uploads[object_.bucket.name][object_.name]:
            part.seek(0)
            file.write(part.read())

    async def delete_object(self, key_id: str, object_: S3Object) -> None:
        del self._objects[object_.bucket.name][object_.name]

    async def delete_bucket(self, key_id: str, bucket: Bucket) -> None:
        if bucket.name in self._objects:
            del self._objects[bucket.name]
        del self._buckets[bucket.name]

    def reset(self) -> None:
        self._buckets = {}
        self._objects = defaultdict(dict)
        self._multipart_uploads = defaultdict(dict)


interface = InMemoryInterface()


@pytest.fixture(autouse=True)
def s3_interface() -> InMemoryInterface:
    interface.reset()
    return interface


@pytest_asyncio.fixture
async def app_with_lifespan(s3_interface: InMemoryInterface):
    app = S3Server(s3_interface)
    async with LifespanManager(app) as manager:
        yield manager.app


@pytest_asyncio.fixture(scope="function")
async def client_factory(app_with_lifespan):
    def _client_factory(*args, **kwargs) -> SignedClient:
        return SignedClient(*args, **kwargs, app=app_with_lifespan, base_url="https://s3server.test")

    return _client_factory


@pytest_asyncio.fixture(scope="function")
async def s3_client(client_factory: Callable[..., SignedClient]):
    return Client("valid_access", "access_key", "https://s3server.test", httpx_client=client_factory)
