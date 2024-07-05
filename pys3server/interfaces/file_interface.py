import json
from hashlib import md5
from os import remove
from os.path import getsize
from pathlib import Path
from typing import TypedDict
from shutil import rmtree

from pys3server import BaseInterface, S3Object, Bucket, BaseReadStream, BaseWriteStream, BucketAlreadyOwnedByYou, \
    BucketAlreadyExists, AccessDenied, NoSuchKey, Part, InvalidPart


class RootMetadata(TypedDict):
    users: dict[str, list[str]]


class BucketMetadata(TypedDict):
    owner: str
    files: list[str]


class FileReadStream(BaseReadStream):
    __slots__ = ("_fp", "_content_range", "_total_size")

    BS = 32 * 1024

    def __init__(self, fp: ..., content_range: tuple[int, int] | None, total_size: int):
        self._fp = fp
        self._read_more = -1
        if content_range is not None:
            self._fp.seek(content_range[0])
            self._read_more = content_range[1] - content_range[0]

        self._total_size = total_size

    async def read(self) -> bytes | None:
        data = self._fp.read(min(self._read_more if self._read_more != -1 else self.BS, self.BS))
        if self._read_more != -1:
            self._read_more -= len(data)

        if not data:
            self._fp.close()

        return data if data else None

    async def supports_range(self) -> bool:
        return True

    async def total_size(self) -> int | None:
        return self._total_size


class FileWriteStream(BaseWriteStream):
    __slots__ = ("_fp", "_metadata_path", "_object_name", "_close",)

    def __init__(self, fp: ..., metadata_path: Path | None = None, object_name: str | None = None, close: bool = True):
        self._fp = fp
        self._metadata_path = metadata_path
        self._object_name = object_name
        self._close = close

    async def write(self, data: bytes | None) -> None:
        if data is None:
            if self._close:
                self._fp.close()

            if self._metadata_path is None or self._object_name is None:
                return

            with open(self._metadata_path) as f:
                meta: BucketMetadata = json.load(f)

            meta["files"].append(self._object_name)

            with open(self._metadata_path, "w") as f:
                json.dump(meta, f)

            return

        self._fp.write(data)


class FileInterface(BaseInterface):
    __slots__ = ("_root_dir", "_users",)

    def __init__(self, root_dir: Path | str, users: dict[str, str]):
        self._root_dir = Path(root_dir)
        self._users = users

    def _get_or_create_root_metadata(self) -> RootMetadata:
        root_metadata = self._root_dir / "metadata.json"
        if root_metadata.exists():
            with open(root_metadata) as f:
                return json.load(f)

        return {"users": {}}

    def _write_root_metadata(self, root_meta: RootMetadata) -> None:
        root_metadata = self._root_dir / "metadata.json"
        root_metadata.parent.mkdir(parents=True, exist_ok=True)

        with open(root_metadata, "w") as f:
            json.dump(root_meta, f)

    def _check_ownership_get_metadata(self, key_id: str, bucket: Bucket) -> BucketMetadata:
        bucket_metadata = self._root_dir / bucket.name / "metadata.json"
        if not bucket_metadata.exists():
            raise NoSuchKey(bucket)

        with open(bucket_metadata) as f:
            meta: BucketMetadata = json.load(f)

        if meta["owner"] != key_id:
            raise AccessDenied(bucket)

        return meta

    async def access_key(self, key_id: str | None, object_: S3Object | Bucket | None) -> str | None:
        bucket = object_.bucket if isinstance(object_, S3Object) else object_
        if bucket is not None:
            self._check_ownership_get_metadata(key_id, bucket)

        return self._users.get(key_id)

    async def create_bucket(self, key_id: str, bucket: str) -> Bucket:
        bucket_metadata = self._root_dir / bucket / "metadata.json"
        if not bucket_metadata.exists():
            bucket_metadata.parent.mkdir(parents=True, exist_ok=True)
            with open(bucket_metadata, "w") as f:
                json.dump({"owner": key_id, "files": []}, f)

            root_meta = self._get_or_create_root_metadata()

            if key_id not in root_meta["users"]:
                root_meta["users"][key_id] = []

            root_meta["users"][key_id].append(bucket)
            self._write_root_metadata(root_meta)

            return Bucket(bucket)

        with open(bucket_metadata) as f:
            meta: BucketMetadata = json.load(f)

        bucket = Bucket(bucket)
        if meta["owner"] == key_id:
            raise BucketAlreadyOwnedByYou(bucket)
        else:
            raise BucketAlreadyExists(bucket)

    async def list_buckets(self, key_id: str) -> list[Bucket]:
        root_meta = self._get_or_create_root_metadata()

        if key_id not in root_meta["users"]:
            return []

        return [Bucket(name) for name in root_meta["users"][key_id]]

    async def list_bucket(self, key_id: str, bucket: Bucket) -> list[S3Object]:
        meta = self._check_ownership_get_metadata(key_id, bucket)

        return [S3Object(bucket, name, getsize(self._root_dir / bucket.name / name)) for name in meta["files"]]

    async def read_object(
            self, key_id: str, object_: S3Object, content_range: tuple[int, int] | None = None
    ) -> FileReadStream:
        self._check_ownership_get_metadata(key_id, object_.bucket)

        object_path = self._root_dir / object_.bucket.name / object_.name
        return FileReadStream(open(object_path, "rb"), content_range, getsize(object_path))

    async def write_object(self, key_id: str, bucket: Bucket, object_name: str, size: int) -> FileWriteStream:
        self._check_ownership_get_metadata(key_id, bucket)

        object_path = self._root_dir / bucket.name / object_name
        object_path.parent.mkdir(parents=True, exist_ok=True)
        return FileWriteStream(open(object_path, "wb"), self._root_dir / bucket.name / "metadata.json", object_name)

    async def create_multipart_upload(self, key_id: str, bucket: Bucket, object_name: str) -> S3Object:
        return S3Object(bucket, object_name, 0)

    async def write_object_multipart(self, object_: S3Object, part_id: int, size: int) -> FileWriteStream:
        part_path = self._root_dir / object_.bucket.name / ".multipart" / f"{object_.name}.part{part_id}"
        part_path.parent.mkdir(parents=True, exist_ok=True)
        return FileWriteStream(open(part_path, "wb"))

    async def finish_multipart_upload(self, object_: S3Object, parts: list[Part]) -> None:
        object_path = self._root_dir / object_.bucket.name / object_.name
        with open(object_path, "wb") as out:
            for part in parts:
                part_path = self._root_dir / object_.bucket.name / ".multipart" / f"{object_.name}.part{part.number}"
                part_hash = md5()
                with open(part_path, "rb") as f:
                    data = f.read()
                    part_hash.update(data)
                    out.write(data)

                if part.etag != part_hash.hexdigest():
                    raise InvalidPart(object_.bucket, object_)

            stream = FileWriteStream(f, self._root_dir / object_.bucket.name / "metadata.json", object_.name, False)
            await stream.write(None)

    async def delete_object(self, key_id: str, object_: S3Object) -> None:
        meta = self._check_ownership_get_metadata(key_id, object_.bucket)
        try:
            meta["files"].remove(object_.name)
        except ValueError:
            return

        remove(self._root_dir / object_.bucket.name / object_.name)
        with open(self._root_dir / object_.bucket.name / "metadata.json", "w") as f:
            json.dump(meta, f)

    async def delete_bucket(self, key_id: str, bucket: Bucket) -> None:
        self._check_ownership_get_metadata(key_id, bucket)

        meta = self._get_or_create_root_metadata()
        try:
            meta["users"][key_id].remove(bucket.name)
        except (ValueError, IndexError):
            return

        rmtree(self._root_dir / bucket.name, ignore_errors=True)
        self._write_root_metadata(meta)
