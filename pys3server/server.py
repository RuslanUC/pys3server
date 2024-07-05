from functools import wraps
from io import BytesIO
from os import urandom
from typing import Callable, Awaitable, cast, AsyncIterable
from xml.etree import ElementTree

from blacksheep import Application, Request, Response, StreamedContent, TextContent, Content

from pys3server import BaseInterface, ListAllMyBucketsResult, Bucket, S3Object, ListBucketResult, \
    InitiateMultipartUploadResult, CompleteMultipartUploadResult, SignatureV4, JWTMpNoTs, ETagWriteStream, Part, \
    InvalidPart, InvalidPartOrder, BaseXmlResponse, parse_query, parse_range
from pys3server.errors import S3Error, InvalidAccessKeyId, NoSuchUpload
from pys3server.xml_utils import get_xml_attr


AsgiReceive = Callable[[], Awaitable[dict]]
AsgiSend = Callable[[dict], Awaitable]


def wrap_method(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await func(*args, **kwargs)

    return wrapper


class S3Server:
    def __init__(self, s3_interface: BaseInterface, jwt_key: bytes | None = None):
        self._interface = s3_interface
        self._jwt_key = jwt_key or urandom(32)

        self._app = Application()
        self._app.router.add_get("/", wrap_method(self._list_buckets))
        self._app.router.add_put("/{bucket_name}", wrap_method(self._create_bucket))
        self._app.router.add_get("/{bucket_name}", wrap_method(self._list_bucket))
        self._app.router.add_delete("/{bucket_name}", wrap_method(self._delete_bucket))
        self._app.router.add_get("/{bucket_name}/{path:object_name}", wrap_method(self._read_object))
        self._app.router.add_head("/{bucket_name}/{path:object_name}", wrap_method(self._read_object_size))
        self._app.router.add_put("/{bucket_name}/{path:object_name}", wrap_method(self._write_object))
        self._app.router.add_post("/{bucket_name}/{path:object_name}", wrap_method(self._create_complete_multipart))
        self._app.router.add_delete("/{bucket_name}/{path:object_name}", wrap_method(self._delete_object))

        self._original_error_handler = self._app.handle_internal_server_error
        self._app.handle_internal_server_error = self._handle_internal_server_error

        self._app.on_start(self._interface.on_start)

    @staticmethod
    def _xml(content: str | BaseXmlResponse, status_code: int = 200) -> Response:
        content = content.to_xml() if isinstance(content, BaseXmlResponse) else content
        return Response(status_code, [(b"Content-Type", b"application/xml")], TextContent(content))

    async def __call__(self, scope: dict, receive: AsgiReceive, send: AsgiSend) -> None:
        return await self._app(scope, receive, send)

    async def _handle_internal_server_error(self, request: Request, exc: Exception) -> Response:
        if isinstance(exc, S3Error):
            return self._xml(exc.to_xml(), exc.status_code)

        return await self._original_error_handler(request, exc)

    async def _auth(self, request: Request, bucket_or_object: S3Object | Bucket | None = None) -> str:
        state = SignatureV4.parse(request)
        key_id = state.key_id.decode("utf8") if state is not None else None
        access_key = await self._interface.access_key(key_id, bucket_or_object)
        if access_key is not None and not state.verify(access_key):
            raise InvalidAccessKeyId()

        return key_id

    async def _list_buckets(self, request: Request) -> Response:
        key_id = await self._auth(request)

        return self._xml(ListAllMyBucketsResult(await self._interface.list_buckets(key_id), key_id))

    async def _create_bucket(self, request: Request, bucket_name: str) -> Response:
        key_id = await self._auth(request)

        bucket = await self._interface.create_bucket(key_id, bucket_name)
        resp = Response(200)
        resp.add_header(b"Location", f"/{bucket.name}".encode())

        return resp

    async def _list_bucket(self, request: Request, bucket_name: str) -> Response:
        bucket = Bucket(bucket_name)
        key_id = await self._auth(request, bucket)

        return self._xml(ListBucketResult(bucket, await self._interface.list_bucket(key_id, bucket)))

    async def _read_object(self, request: Request, bucket_name: str, object_name: str) -> Response:
        object_ = S3Object(Bucket(bucket_name), object_name, 0)
        key_id = await self._auth(request, object_)
        range_ = parse_range(request.headers.get_first(b"Range"))
        stream = await self._interface.read_object(key_id, object_, range_)

        async def _provider():
            while (data := await stream.read()) is not None:
                yield data

        status = 200 if range_ is None or not await stream.supports_range() else 206
        size = await stream.total_size()
        headers = []
        # if size is not None:
        #    headers.append((b"Content-Length", str(size).encode("utf8")))
        if range_ is not None and await stream.supports_range():
            size = size if size is not None else "*"
            headers.append((b"Content-Range", f"bytes {range_[0]}-{range_[1]}/{size}".encode("utf8")))

        return Response(status, headers, StreamedContent(b"application/octet-stream", _provider))

    async def _read_object_size(self, request: Request, bucket_name: str, object_name: str) -> Response:
        object_ = S3Object(Bucket(bucket_name), object_name, 0)
        key_id = await self._auth(request, object_)
        stream = await self._interface.read_object(key_id, object_)

        size = await stream.total_size()
        headers = []
        if size is not None:
            headers.append((b"Content-Length", str(size).encode("utf8")))

        return Response(200, headers, Content(b"application/octet-stream", b""))

    async def _write_object(self, request: Request, bucket_name: str, object_name: str) -> Response:
        upload_info = None
        query = parse_query(request)
        bucket = Bucket(bucket_name)
        if "uploadId" in query:
            if "partNumber" not in query or not query["partNumber"].isdigit():
                raise NoSuchUpload(bucket)
            if (upload_info := JWTMpNoTs.decode(query["uploadId"], self._jwt_key)) is None:
                raise NoSuchUpload(bucket)
            if upload_info["bucket"] != bucket_name:
                raise NoSuchUpload(bucket)
            if upload_info["object"] != object_name:
                raise NoSuchUpload(bucket)

        key_id = await self._auth(request, bucket)
        if upload_info is not None:
            if upload_info["key_id"] != key_id:
                raise NoSuchUpload(bucket)

        content_length = int(request.headers.get_first(b"Content-Length") or 0)

        if upload_info is None:
            stream = await self._interface.write_object(key_id, bucket, object_name, content_length)
        else:
            object_ = S3Object(bucket, object_name, 0)
            stream = await self._interface.write_object_multipart(object_, int(query["partNumber"]), content_length)

        stream = stream if isinstance(stream, ETagWriteStream) else ETagWriteStream(stream)
        async for chunk in cast(AsyncIterable[bytes], request.stream()):
            if chunk:
                await stream.write(chunk)

        await stream.write(None)
        resp = Response(200)
        resp.add_header(b"ETag", stream.hexdigest().encode())

        return resp

    async def _create_complete_multipart(self, request: Request, bucket_name: str, object_name: str) -> Response:
        query = parse_query(request)
        bucket = Bucket(bucket_name)
        object_ = S3Object(bucket, object_name, 0)

        if "uploads" not in query and "uploadId" not in query:
            raise NoSuchUpload(bucket, object_)

        key_id = await self._auth(request, bucket)

        if "uploads" in query:
            obj = await self._interface.create_multipart_upload(key_id, bucket, object_name)
            upload_id = JWTMpNoTs.encode({"bucket": bucket.name, "object": obj.name, "key_id": key_id}, self._jwt_key)

            return self._xml(InitiateMultipartUploadResult(obj, upload_id))
        elif "uploadId" in query:
            if (upload_info := JWTMpNoTs.decode(query["uploadId"], self._jwt_key)) is None:
                raise NoSuchUpload(bucket)
            if upload_info["bucket"] != bucket_name:
                raise NoSuchUpload(bucket)
            if upload_info["object"] != object_name:
                raise NoSuchUpload(bucket)
            if upload_info["key_id"] != key_id:
                raise NoSuchUpload(bucket)

            parts: list[Part] = []
            data = await request.read()
            req = ElementTree.parse(BytesIO(data)).getroot()
            for part in get_xml_attr(req, "Part", True):
                parts.append(Part(
                    int(get_xml_attr(part, "PartNumber").text),
                    get_xml_attr(part, "ETag").text,
                ))

            last_number = 0
            for part in parts:
                if part.number <= last_number:
                    raise InvalidPartOrder(bucket, object_)
                if part.number - 1 != last_number:
                    raise InvalidPart(bucket, object_)
                last_number = part.number

            await self._interface.finish_multipart_upload(object_, parts)
            return self._xml(CompleteMultipartUploadResult(object_))

    async def _delete_object(self, request: Request, bucket_name: str, object_name: str) -> Response:
        object_ = S3Object(Bucket(bucket_name), object_name, 0)
        key_id = await self._auth(request, object_)
        await self._interface.delete_object(key_id, object_)

        return Response(204)

    async def _delete_bucket(self, request: Request, bucket_name: str) -> Response:
        bucket = Bucket(bucket_name)
        key_id = await self._auth(request, bucket)
        await self._interface.delete_bucket(key_id, bucket)

        return Response(204)
