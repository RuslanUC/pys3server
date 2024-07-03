class Bucket:
    __slots__ = ("name", "creation_timestamp")

    def __init__(self, name: str, creation_timestamp: int = 0):
        self.name = name
        self.creation_timestamp = creation_timestamp

    def to_xml(self) -> str:
        return f"<Bucket><Name>{self.name}</Name></Bucket>"
