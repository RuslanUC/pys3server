from abc import ABC, abstractmethod

from pys3server import S3Object, Bucket, Part


class BaseReadStream:
    @abstractmethod
    async def read(self) -> bytes | None:
        """
        Reads object's content

        :return: Bytes (content) or None if EOF is reached
        """

    @abstractmethod
    async def supports_range(self) -> bool:
        """
        Whether this stream supports reading parts of an object.

        :return: True if the stream supports reading parts of an object, False otherwise
        """

    @abstractmethod
    async def total_size(self) -> int | None:
        """
        Total size of a whole object (not just part that specified in "range" argument)

        :return: Total size of an object
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
    async def on_start(self) -> None:
        """
        Called when the application is started

        :return: None
        """

    @abstractmethod
    async def access_key(self, key_id: str | None, object_: S3Object | Bucket | None) -> str | None:
        """
        Checks credentials are correct.
        Also checks if the user with given key_id has access to the object/bucket:
         - If key id is valid and has access to object/bucket, return it's access key
         - If key id is valid and don't have access to object/bucket, raise AccessDenied
         - If key id is invalid, raise InvalidAccessKeyId
         - If key id is None and object/bucket is public, return None
         - If key id is None and object/bucket is private, raise AccessDenied

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
    async def write_object(self, key_id: str, bucket: Bucket, object_name: str, size: int) -> BaseWriteStream:
        """
        Writes object content

        :param key_id: S3 access key id
        :param bucket: S3 bucket
        :param object_name: S3 object name
        :param size: Object size in bytes
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
    async def write_object_multipart(self, object_: S3Object, part_id: int, size: int) -> BaseWriteStream:
        """
        Writes object's part content

        :param object_: S3 object
        :param part_id: Part number
        :param size: Part size in bytes
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

    @abstractmethod
    async def delete_object(self, key_id: str, object_: S3Object) -> None:
        """
        Deletes object

        :param key_id: S3 access key id
        :param object_: S3 object to delete
        :return: None
        """

    @abstractmethod
    async def delete_bucket(self, key_id: str, bucket: Bucket) -> None:
        """
        Deletes bucket

        :param key_id: S3 access key id
        :param bucket: S3 bucket to delete
        :return: None
        """
