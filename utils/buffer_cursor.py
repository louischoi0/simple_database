import mmap
import os
from utils.dec import *


# ─────────────────────────────────────────────
#  내부 백엔드 추상
# ─────────────────────────────────────────────

class _MemoryBackend:
    """기존 bytearray 기반 백엔드 (변경 없음)."""

    def __init__(self, buf: bytearray):
        self._buf = buf

    # ── 크기 / 슬라이스 ──────────────────────
    def __len__(self):
        return len(self._buf)

    def __getitem__(self, key):
        return self._buf[key]

    def __setitem__(self, key, value):
        self._buf[key] = value

    # ── 추가 연산 ────────────────────────────
    def append(self, data: bytes | bytearray):
        self._buf += data

    def close(self):
        pass  # 메모리 백엔드는 리소스 해제 불필요


class _MmapBackend:
    """
    mmap 기반 파일 백엔드.

    파일이 존재하지 않거나 크기가 0이면 size 바이트로 초기화한다.
    이후 resize()로 파일을 확장할 수 있다.

    Parameters
    ----------
    path     : 파일 경로
    size     : 초기 파일 크기 (파일이 없을 때만 사용)
    writable : False면 읽기 전용 mmap
    """

    def __init__(self, path: str, size: int = 0, writable: bool = True):
        self._path = path
        self._writable = writable
        self._mm: mmap.mmap | None = None
        self._fp = None
        self._open(size)

    # ── 내부 open/remap ──────────────────────
    def _open(self, initial_size: int):
        mode = "r+b" if os.path.exists(self._path) else "w+b"

        self._fp = open(self._path, mode)

        # 파일이 비어있으면 initial_size로 확장
        self._fp.seek(0, 2)          # EOF
        file_size = self._fp.tell()
        if file_size == 0 and initial_size > 0:
            self._fp.seek(initial_size - 1)
            self._fp.write(b"\x00")
            self._fp.flush()

        self._remap()

    def _remap(self):
        """현재 파일 크기에 맞춰 mmap을 (재)생성한다."""
        if self._mm is not None:
            self._mm.close()

        self._fp.seek(0, 2)
        file_size = self._fp.tell()

        if file_size == 0:
            self._mm = None
            return

        access = mmap.ACCESS_WRITE if self._writable else mmap.ACCESS_READ
        self._mm = mmap.mmap(self._fp.fileno(), 0, access=access)

    # ── 크기 / 슬라이스 ──────────────────────
    def __len__(self):
        return 0 if self._mm is None else len(self._mm)

    def __getitem__(self, key):
        if self._mm is None:
            raise IndexError("mmap is empty")
        return self._mm[key]

    def __setitem__(self, key, value):
        if self._mm is None:
            raise IndexError("mmap is empty")
        self._mm[key] = value

    # ── 추가 연산 ────────────────────────────
    def append(self, data: bytes | bytearray):
        """
        파일 끝에 데이터를 추가한다.
        mmap을 flush → 파일 확장 → remap 순으로 처리한다.
        """
        if self._mm is not None:
            self._mm.flush()

        extra = len(data)
        self._fp.seek(0, 2)
        self._fp.write(data)
        self._fp.flush()
        self._remap()

    def resize(self, new_size: int):
        """
        파일(과 mmap)을 new_size 바이트로 확장한다.
        축소는 지원하지 않는다.
        """
        if new_size <= len(self):
            return

        if self._mm is not None:
            self._mm.flush()

        self._fp.seek(new_size - 1)
        self._fp.write(b"\x00")
        self._fp.flush()
        self._remap()

    def flush(self):
        if self._mm is not None:
            self._mm.flush()

    def close(self):
        if self._mm is not None:
            self._mm.flush()
            self._mm.close()
            self._mm = None
        if self._fp is not None:
            self._fp.close()
            self._fp = None

    # ── context manager ──────────────────────
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ─────────────────────────────────────────────
#  buffer_cursor
# ─────────────────────────────────────────────

class buffer_cursor:
    def __init__(self, buffer=None, id=None, _backend=None):
        if _backend is not None:
            # from_file() 경로
            self._backend = _backend
        else:
            buf = buffer if buffer is not None else bytearray()
            self._backend = _MemoryBackend(buf)

        self.c = 0
        self.id = id

    @classmethod
    def from_file(
        cls,
        path: str,
        size: int = 0,
        writable: bool = True,
        id=None,
    ) -> "buffer_cursor":
        backend = _MmapBackend(path, size=size, writable=writable)
        return cls(_backend=backend, id=id)

    @property
    def buffer(self):
        if isinstance(self._backend, _MemoryBackend):
            return self._backend._buf
        raise AttributeError(
            "buffer 속성은 메모리 백엔드에서만 사용 가능합니다. "
            "파일 백엔드에서는 mmap 객체를 직접 사용하세요."
        )

    @buffer.setter
    def buffer(self, value):
        if isinstance(self._backend, _MemoryBackend):
            self._backend._buf = value
        else:
            raise AttributeError("파일 백엔드에서는 buffer를 직접 교체할 수 없습니다.")

    # ── context manager ──────────────────────

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        """파일 백엔드는 flush + close. 메모리 백엔드는 no-op."""
        self._backend.close()

    def flush(self):
        """파일 백엔드의 mmap을 디스크에 동기화한다."""
        if isinstance(self._backend, _MmapBackend):
            self._backend.flush()

    def resize(self, new_size: int):
        """
        파일 백엔드의 크기를 확장한다.
        메모리 백엔드에서는 무시된다.
        """
        if isinstance(self._backend, _MmapBackend):
            self._backend.resize(new_size)

    # ── 경계 검사 ────────────────────────────

    def check(self, length):
        if self.c + length > len(self._backend):
            raise Exception(
                f"buffer cursor exceeded: {self.c + length} of {len(self._backend)}"
            )

    # ── 로깅 ─────────────────────────────────

    def read_log(self, size, v):
        if not BUFFER_CURSOR_DEBUG:
            return
        self.log(f"read buffer id={self.id} pos={self.c}; len={size}; v={v}")

    def write_log(self, size, v):
        if not BUFFER_CURSOR_DEBUG:
            return
        self.log(f"write buffer id={self.id} pos={self.c}; len={size}; v={v}")

    def log(self, *args):
        if not BUFFER_CURSOR_DEBUG:
            return
        print(*args)

    # ── 포인터 조작 ──────────────────────────

    def at(self, pos):
        self.c = pos

    def advance(self, length):
        if length < 0:
            raise Exception("advance length must be greater than zero or equal to zero")
        if length == 0:
            return
        self.check(length)
        buf = self._backend[self.c : self.c + length]
        self.c += length
        self.log(f"cursor advance id={self.id} len={length}; now={self.c}")
        return buf

    def end(self):
        return self.c == len(self._backend)

    def tail(self):
        return self._backend[self.c :]

    # ── 읽기 ─────────────────────────────────

    def read(self, size):
        self.check(size)
        buf = self._backend[self.c : self.c + size]
        self.c += size
        return buf

    def read_int32(self):
        self.check(4)
        buf = self._backend[self.c : self.c + 4]
        v = toint32(buf)
        self.read_log(4, v)
        self.c += 4
        return v

    def read_int64(self):
        self.check(8)
        buf = self._backend[self.c : self.c + 8]
        v = toint64(buf)
        self.read_log(8, v)
        self.c += 8
        return v

    def read_bytes(self):
        length = self.read_int64()
        if length == 0:
            return None
        self.check(length)
        value = self._backend[self.c : self.c + length]
        self.c += length
        return value

    def read_varchar(self):
        length = self.read_int64()
        buf = self._backend[self.c : self.c + length]
        self.c += length
        return bytes(buf).decode("utf-8")

    def read_char(self, length):
        self.check(length)
        buf = self._backend[self.c : self.c + length]
        self.c += length
        return bytes(buf).decode("utf-8")

    def read_bool(self):
        self.check(1)
        v = self._backend[self.c : self.c + 1]
        self.c += 1
        return tobit(v)

    # ── 쓰기 (check 포함) ────────────────────

    def write_int32(self, v):
        self.check(4)
        return self.write_int32_a(v)

    def write_int64(self, v):
        self.check(8)
        return self.write_int64_a(v)

    def write_varchar(self, string):
        data = string.encode("utf-8")
        length = len(data)
        self.write_int64(length)
        self.check(length)
        self._backend[self.c : self.c + length] = data
        self.c += length
        return length

    def write_bytes(self, buffer):
        if buffer is None:
            return self.write_int64(0)
        length = len(buffer)
        self.check(length + 8)
        self.write_int64(length)
        return self.write_raw(buffer)

    def write_bool(self, value):
        value = int(value)
        data = serbit(value)
        self.check(1)
        self._backend[self.c : self.c + 1] = data
        self.c += 1

    def write_char(self, value, length):
        self.check(length)
        self._backend[self.c : self.c + length] = value.encode("utf-8")
        self.c += length

    def write_raw_a(self, buffer):
        """size prefix 없이 raw bytes를 쓴다."""
        length = len(buffer)
        self._backend[self.c : self.c + length] = buffer
        self.c += length

    def write_raw(self, buffer):
        """size prefix 없이 raw bytes를 쓴다."""
        length = len(buffer)
        self.check(length)
        self._backend[self.c : self.c + length] = buffer
        self.c += length

    def pad(self, size):
        self.check(size)
        return self.pad_a(size)

    # ── 쓰기 (_a: check 생략, 빠른 경로) ────

    def write_int32_a(self, v):
        self._backend[self.c : self.c + 4] = serint32(v)
        self.write_log(4, v)
        self.c += 4

    def write_int64_a(self, v):
        self._backend[self.c : self.c + 8] = serint64(v)
        self.write_log(8, v)
        self.c += 8

    def write_varchar_a(self, string):
        data = string.encode("utf-8")
        length = len(data)
        self.write_int64_a(length)
        self._backend[self.c : self.c + length] = data
        self.c += length
        return length

    def write_bytes_a(self, buffer):
        if buffer is None:
            return self.write_int64_a(0)
        length = len(buffer)
        self.write_int64_a(length)
        self._backend[self.c : self.c + length] = buffer
        self.c += length

    def write_bool_a(self, value):
        value = int(value)
        data = serbit(value)
        self._backend[self.c : self.c + 1] = data
        self.c += 1

    def write_char_a(self, value, length):
        self._backend[self.c : self.c + length] = value.encode("utf-8")
        self.c += length

    def pad_a(self, size):
        self._backend[self.c : self.c + size] = b"\x00" * size
        self.log(f"pad buffer id={self.id} pos={self.c}; len={size}")
        self.c += size
        return size

    # ── append (백엔드에 위임) ───────────────

    def append(self, buffer):
        self._backend.append(buffer)
        self.c = len(self._backend)

    # ── dynamic type ─────────────────────────

    def write_dynamic_type_a(self, type_val, value, size=None):
        from core.catalog import get_type_value

        if type_val == get_type_value("int"):
            return self.write_int64_a(value)
        elif type_val == get_type_value("bool"):
            return self.write_bool_a(value)
        elif type_val == get_type_value("varchar"):
            return self.write_varchar_a(value)
        elif type_val == get_type_value("char"):
            assert size is not None
            return self.write_char_a(value, size)
        elif type_val == get_type_value("bytes"):
            return self.write_bytes_a(value)
        else:
            raise Exception(f"invalid type value {type_val}")

    def read_dynamic_type_a(self, type_val, size=None):
        from core.catalog import get_type_value

        if type_val == get_type_value("int"):
            return self.read_int64()
        elif type_val == get_type_value("bool"):
            return self.read_bool()
        elif type_val == get_type_value("varchar"):
            return self.read_varchar()
        elif type_val == get_type_value("char"):
            return self.read_char(size)
        elif type_val == get_type_value("bytes"):
            return self.read_bytes()
        else:
            raise Exception(f"invalid type value {type_val}")