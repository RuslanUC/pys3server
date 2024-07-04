from abc import abstractmethod, ABC

from pys3server import Bucket, S3Object
from pys3server.xml_utils import NS_URL


class BaseXmlResponse(ABC):
    @abstractmethod
    def to_xml(self) -> str:
        ...


class ListAllMyBucketsResult(BaseXmlResponse):
    __slots__ = ("buckets", "key_id",)

    def __init__(self, buckets: list[Bucket], key_id: str):
        self.buckets = buckets
        self.key_id = key_id

    def to_xml(self) -> str:
        buckets = "".join([bucket.to_xml() for bucket in self.buckets])

        return (
            f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            f"<ListAllMyBucketsResult xmlns=\"{NS_URL}\">"
            f"<Buckets>{buckets}</Buckets>"
            f"<Owner><ID>{self.key_id}</ID></Owner>"
            f"</ListAllMyBucketsResult>"
        )


class ListBucketResult(BaseXmlResponse):
    __slots__ = ("bucket", "objects",)

    def __init__(self, bucket: Bucket, objects: list[S3Object]):
        self.bucket = bucket
        self.objects = objects

    def to_xml(self) -> str:
        objects = "".join([f"<Contents>{obj.to_xml()}</Contents>" for obj in self.objects])

        return (
            f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            f"<ListBucketResult xmlns=\"{NS_URL}\">"
            f"<IsTruncated>false</IsTruncated>"
            f"<Name>{self.bucket.name}</Name>"
            f"{objects}"
            f"</ListBucketResult>"
        )


class InitiateMultipartUploadResult(BaseXmlResponse):
    __slots__ = ("object", "upload_id",)

    def __init__(self, object_: S3Object, upload_id: str):
        self.object = object_
        self.upload_id = upload_id

    def to_xml(self) -> str:
        return (
            f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            f"<InitiateMultipartUploadResult xmlns=\"{NS_URL}\">"
            f"<Bucket>{self.object.bucket.name}</Bucket>"
            f"<Key>{self.object.name}</Key>"
            f"<UploadId>{self.upload_id}</UploadId>"
            f"</InitiateMultipartUploadResult>"
        )


class CompleteMultipartUploadResult(BaseXmlResponse):
    __slots__ = ("object",)

    def __init__(self, object_: S3Object):
        self.object = object_

    def to_xml(self) -> str:
        return (
            f"<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            f"<CompleteMultipartUploadResult xmlns=\"{NS_URL}\">"
            f"<Bucket>{self.object.bucket.name}</Bucket>"
            f"<Key>{self.object.name}</Key>"
            f"</CompleteMultipartUploadResult>"
        )
