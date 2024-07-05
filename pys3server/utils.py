from urllib.parse import unquote

from blacksheep import Request


def parse_query(request: Request, decode: bool = True) -> dict[str, str] | dict[bytes, bytes]:
    result = {}
    for kv in request._raw_query.split(b"&"):
        if not kv:
            continue

        kv = kv.split(b"=", 1)

        if len(kv) == 1:
            kv.append(b"")
        k, v = kv
        k, v, = unquote(k), unquote(v)
        if not decode:
            k, v = k.encode("utf8"), v.encode("utf8")
        result[k] = v

    return result


def parse_range(header: bytes | None) -> tuple[int, int] | None:
    if header is None:
        return

    header = header.split(b",")[0].split(b"=")
    if len(header) != 2 or header[0] != b"bytes":
        return

    start, end = header[1].split(b"-")
    if not start.isdigit() or not end.isdigit():  # TODO: support ranges like "500-" or "-250"
        return

    return int(start), int(end)
