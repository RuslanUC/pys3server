from abc import ABC, abstractmethod

from pys3server import S3Object, Bucket, Part


class BaseReadStream:
    @abstractmethod
    async def read(self) -> bytes | None:
        """
        Reads object's content

        :return: Bytes (content) or None if EOF is reached
        """


class BaseWriteStream:
    @abstractmethod
    async def write(self, content: bytes | None) -> None:
        """
        Writes object's content

        :param content: Content to write or None if EOF is reached
        :return: None
        """


class BaseInterface(ABC):
    @abstractmethod
    async def access_key(self, key_id: str | None, object_: S3Object | Bucket | None) -> str | None:
        """
        Checks credentials are correct.
        Also checks if the user with given key_id has access to the object/bucket.

        :param key_id: S3 access key id
        :param object_: S3 object/bucket (or None if operation doesn't need a s3 object (e.g. ListBuckets))
        :return: Access key if key_id exists and has access to object_, None otherwise
        """

    @abstractmethod
    async def create_bucket(self, key_id: str, bucket: str) -> Bucket:
        """
        Creates new bucket for user with given key id

        :param key_id: S3 access key id
        :param bucket: Bucket name
        :return: Created bucket
        """

    @abstractmethod
    async def list_buckets(self, key_id: str) -> list[Bucket]:
        """
        Lists all buckets for user with given key id

        :param key_id: S3 access key id
        :return: List of buckets
        """

    @abstractmethod
    async def list_bucket(self, key_id: str, bucket: Bucket) -> list[S3Object]:
        """
        Lists all objects in given bucket for user with given key id

        :param key_id: S3 access key id
        :param bucket: S3 bucket
        :return: List of objects in given bucket
        """

    @abstractmethod
    async def read_object(
            self, key_id: str, object_: S3Object, content_range: tuple[int, int] | None = None
    ) -> BaseReadStream:
        """
        Reads object content

        :param key_id: S3 access key id
        :param object_: S3 object
        :param content_range: Range of bytes to return
        :return: BaseReadStream from which file content will be read
        """

    @abstractmethod
    async def write_object(self, key_id: str, bucket: Bucket, object_name: str) -> BaseWriteStream:
        """
        Writes object content

        :param key_id: S3 access key id
        :param bucket: S3 bucket
        :param object_name: S3 object name
        :return: BaseWriteStream to which object's content will be written
        """

    @abstractmethod
    async def create_multipart_upload(self, key_id: str, bucket: Bucket, object_name: str) -> S3Object:
        """
        Creates multipart upload

        :param key_id: S3 access key id
        :param bucket: S3 bucket
        :param object_name: S3 object name
        :return: Created s3 object
        """

    @abstractmethod
    async def write_object_multipart(self, object_: S3Object, part_id: int) -> BaseWriteStream:
        """
        Writes object's part content

        :param object_: S3 object
        :param part_id: Part number
        :return: BaseWriteStream to which object's part's content will be written
        """

    @abstractmethod
    async def finish_multipart_upload(self, object_: S3Object, parts: list[Part]) -> None:
        """
        Finishes multipart upload

        :param object_: S3 object
        :param parts: List of upload parts
        :return: None
        """
