from core.const import *

def serint64(value: int) -> bytes:
    return value.to_bytes(8, byteorder=BYTE_ORDER, signed=True)

def serint32(value: int) -> bytes:
    return value.to_bytes(4, byteorder=BYTE_ORDER, signed=True)

def toint32(buf: bytes) -> int:
    return int.from_bytes(buf, byteorder=BYTE_ORDER, signed=True)

def toint64(buf: bytes) -> int:
    return int.from_bytes(buf, byteorder=BYTE_ORDER, signed=True)

def serbit(value: int) -> bytes:
    return value.to_bytes(1, byteorder=BYTE_ORDER, signed=True)

def tobit(buf: bytes) -> int:
    return int.from_bytes(buf, byteorder=BYTE_ORDER, signed=True)
