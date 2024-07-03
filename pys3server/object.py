from datetime import datetime

from dateutil.tz import UTC

from pys3server import Bucket


class S3Object:
    __slots__ = ("bucket", "name", "size", "modified_at")

    def __init__(self, bucket: Bucket, name: str, size: int, modified_at: int = 0):
        self.bucket = bucket
        self.name = name
        self.size = size
        self.modified_at = modified_at

    def to_xml(self) -> str:
        modified = datetime.fromtimestamp(self.modified_at, UTC).isoformat()
        return f"<Key>{self.name}</Key><Size>{self.size}</Size><LastModified>{modified}</LastModified>"
