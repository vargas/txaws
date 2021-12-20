from typing import AnyStr


def to_str(str_or_bytes: AnyStr, encoding: str = "utf-8") -> str:
    if isinstance(str_or_bytes, str):
        return str_or_bytes
    return str_or_bytes.decode(encoding)


def to_bytes(str_or_bytes: AnyStr, encoding: str = "utf-8") -> bytes:
    if isinstance(str_or_bytes, bytes):
        return str_or_bytes
    return str_or_bytes.encode(encoding)