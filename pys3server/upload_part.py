class Part:
    __slots__ = ("number", "etag",)

    def __init__(self, number: int, etag: str):
        self.number = number
        self.etag = etag
