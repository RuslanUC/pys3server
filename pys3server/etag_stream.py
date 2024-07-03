from hashlib import md5

from pys3server import BaseWriteStream


class ETagWriteStream(BaseWriteStream):
    __slots__ = ("_stream", "_hash",)

    def __init__(self, real_stream: BaseWriteStream):
        self._stream = real_stream
        self._hash = md5()

    async def write(self, content: bytes | None) -> None:
        if content is not None:
            self._hash.update(content)

        await self._stream.write(content)

    def hexdigest(self) -> str:
        return self._hash.hexdigest()
