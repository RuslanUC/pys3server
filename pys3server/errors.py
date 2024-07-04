from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pys3server import Bucket, S3Object


class S3Error(Exception):
    def __init__(self, status_code: int, code: str, message: str, resource: str, details: dict | None = None):
        self.status_code = status_code
        self.code = code
        self.message = message
        self.resource = resource
        self.details = details or {}

    def to_xml(self) -> str:
        details = "".join([f"<{key}>{value}</{key}>" for key, value in self.details.items()])

        return (
            f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            f"<Error>"
            f"<Code>{self.code}</Code>"
            f"<Message>{self.message}</Message>"
            f"<Resource>{self.resource}</Resource>"
            f"{details}"
            f"</Error>"
        )

    @staticmethod
    def _resource(bucket: Bucket | None = None, object_: S3Object | None = None) -> str:
        resource = "/"
        if bucket is not None:
            resource += f"{bucket.name}/"
            if object_ is not None:
                resource += f"{object_.name}"

        return resource

    @staticmethod
    def _details(bucket: Bucket | None = None, object_: S3Object | None = None) -> dict[str, str]:
        details = {}
        if bucket is not None:
            details["BucketName"] = bucket.name
        if object_ is not None:
            details["Key"] = object_.name

        return details


class AccessDenied(S3Error):
    def __init__(self, bucket: Bucket | None = None, object_: S3Object | None = None):
        super().__init__(
            status_code=403,
            code="AccessDenied",
            message="Access Denied.",
            resource=self._resource(bucket, object_),
            details=self._details(bucket, object_),
        )


class BucketAlreadyExists(S3Error):
    def __init__(self, bucket: Bucket):
        super().__init__(
            status_code=409,
            code="BucketAlreadyExists",
            message="The requested bucket name is not available.",
            resource=self._resource(bucket, None),
            details=self._details(bucket, None),
        )


class BucketAlreadyOwnedByYou(S3Error):
    def __init__(self, bucket: Bucket):
        super().__init__(
            status_code=409,
            code="BucketAlreadyOwnedByYou",
            message="The bucket you tried to create already exists, and you own it.",
            resource=self._resource(bucket, None),
            details=self._details(bucket, None),
        )


class NoSuchKey(S3Error):
    def __init__(self, bucket: Bucket, object_: S3Object | None = None):
        super().__init__(
            status_code=404,
            code="NoSuchKey",
            message="The resource you requested does not exist.",
            resource=self._resource(bucket, object_),
            details=self._details(bucket, object_),
        )


class InvalidPart(S3Error):
    def __init__(self, bucket: Bucket, object_: S3Object):
        super().__init__(
            status_code=400,
            code="InvalidPart",
            message="One or more of the specified parts could not be found.",
            resource=self._resource(bucket, object_),
            details=self._details(bucket, object_),
        )


class InvalidPartOrder(S3Error):
    def __init__(self, bucket: Bucket, object_: S3Object):
        super().__init__(
            status_code=400,
            code="InvalidPartOrder",
            message="The list of parts was not in ascending order.",
            resource=self._resource(bucket, object_),
            details=self._details(bucket, object_),
        )
