from functools import wraps

from blacksheep import Application, Request, Response, StreamedContent

from pys3server import BaseInterface, ListAllMyBucketsResult, Bucket, S3Object, ListBucketResult, \
    InitiateMultipartUploadResult, CompleteMultipartUploadResult, SignatureV4, JWTMpNoTs, ETagWriteStream


def wrap_method(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await func(*args, **kwargs)

    return wrapper


class S3Server:
    def __init__(self, s3_interface: BaseInterface):
        self._interface = s3_interface

        self._app = Application()
        self._app.router.add_get("/", wrap_method(self._list_buckets))
        self._app.router.add_put("/{bucket_name}", wrap_method(self._create_bucket))
        self._app.router.add_get("/{bucket_name}", wrap_method(self._list_bucket))
        self._app.router.add_get("/{bucket_name}/{path:object_name}", wrap_method(self._read_object))
        self._app.router.add_put("/{bucket_name}/{path:object_name}", wrap_method(self._write_object))
        self._app.router.add_post("/{bucket_name}/{path:object_name}", wrap_method(self._create_multipart))

    async def _auth(self, request: Request, bucket_or_object: S3Object | Bucket | None = None) -> str:
        state = SignatureV4.parse(request)
        key_id = state.key_id.decode("utf8") if state is not None else None
        access_key = await self._interface.access_key(key_id, bucket_or_object)
        state.verify(access_key)

        return key_id

    async def _list_buckets(self, request: Request) -> str:
        key_id = await self._auth(request)

        return ListAllMyBucketsResult(await self._interface.list_buckets(key_id), key_id).to_xml()

    async def _create_bucket(self, request: Request, bucket_name: str) -> Response:
        key_id = await self._auth(request)

        bucket = await self._interface.create_bucket(key_id, bucket_name)
        resp = Response(200)
        resp.add_header(b"Location", f"/{bucket.name}".encode())

        return resp

    async def _list_bucket(self, request: Request, bucket_name: str) -> str:
        bucket = Bucket(bucket_name)
        key_id = await self._auth(request, bucket)

        return ListBucketResult(bucket, await self._interface.list_bucket(key_id, bucket)).to_xml()

    async def _read_object(self, request: Request, bucket_name: str, object_name: str) -> Response:
        object_ = S3Object(Bucket(bucket_name), object_name, 0)
        await self._auth(request, object_)
        stream = await self._interface.read_object(object_)

        async def _provider():
            while (data := await stream.read()) is not None:
                yield data

        return Response(200, content=StreamedContent(b"application/octet-stream", _provider))

    async def _write_object(self, request: Request, bucket_name: str, object_name: str) -> Response:
        upload_info = None
        query = {k: v for k, v in [kv.decode("utf8").split("=") for kv in request._raw_query.split(b"&")]}
        if "uploadId" in query:
            assert "partNumber" in query and query["partNumber"].isdigit()
            upload_info = JWTMpNoTs.decode(query["uploadId"], b"123456")
            assert upload_info["bucket"] == bucket_name
            assert upload_info["object"] == object_name

        bucket = Bucket(bucket_name)
        key_id = await self._auth(request, bucket)
        if upload_info is not None:
            assert upload_info["key_id"] == key_id

        if upload_info is None:
            stream = await self._interface.write_object(bucket, object_name)
        else:
            object_ = S3Object(bucket, object_name, 0)
            stream = await self._interface.write_object_multipart(object_, int(query["partNumber"]))

        stream = stream if isinstance(stream, ETagWriteStream) else ETagWriteStream(stream)
        async for chunk in request.stream():
            if chunk:
                await stream.write(chunk)

        await stream.write(None)
        resp = Response(200)
        resp.add_header(b"ETag", stream.hexdigest().encode())

        return resp

    async def _create_multipart(self, request: Request, bucket_name: str, object_name: str) -> str:
        query = {k: v for k, v in [kv.decode("utf8").split("=") for kv in request._raw_query.split(b"&")]}
        if "uploads" not in query and "uploadId" not in query:
            print(query)
            assert False

        bucket = Bucket(bucket_name)
        object_ = S3Object(bucket, object_name, 0)
        key_id = await self._auth(request, bucket)

        if "uploads" in query:
            obj = await self._interface.create_multipart_upload(bucket, object_name)
            upload_id = JWTMpNoTs.encode({"bucket": bucket.name, "object": obj.name, "key_id": key_id}, b"123456")

            return InitiateMultipartUploadResult(obj, upload_id).to_xml()
        elif "uploadId" in query:
            upload_info = JWTMpNoTs.decode(query["uploadId"], b"123456")
            assert upload_info["bucket"] == bucket_name
            assert upload_info["object"] == object_name
            assert upload_info["key_id"] == key_id

            # TODO: check etags
            await self._interface.finish_multipart_upload(object_)

            return CompleteMultipartUploadResult(object_).to_xml()
