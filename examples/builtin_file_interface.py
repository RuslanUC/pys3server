from pys3server import S3Server
from pys3server.interfaces import FileInterface


app = S3Server(FileInterface("s3_test", {"idk": "idk"}))
