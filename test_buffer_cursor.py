import os
import tempfile
from utils.buffer_cursor import buffer_cursor


def test_buffer_cursor_rwint64():
    cursor_w = buffer_cursor()
    cursor_r = buffer_cursor()
    str0 = "thisisname"
    print(str0.encode("utf-8"))
    num0 = 0
    bnum0 = b'00000'
    cursor_w.write_int64_a(num0)
    cursor_w.write_varchar_a(str0)
    cursor_w.write_int64_a(num0)
    cursor_w.write_bytes_a(bnum0)
    cursor_w.write_int64_a(num0)
    print(cursor_w.buffer)
    cursor_r.buffer = cursor_w.buffer
    assert cursor_r.read_int64() == num0
    assert cursor_r.read_varchar() == str0
    assert cursor_r.read_int64() == num0
    assert cursor_r.read_bytes() == bnum0
    assert cursor_r.read_int64() == num0


def test_buffer_cursor_bool():
    cursor_w = buffer_cursor()
    cursor_r = buffer_cursor()
    cursor_w.write_bool_a(True)
    cursor_w.write_bool_a(False)
    cursor_w.write_bool_a(True)
    cursor_r.buffer = cursor_w.buffer
    assert cursor_r.read_bool()
    assert not cursor_r.read_bool()
    assert cursor_r.read_bool()


def test_buffer_cursor_bytes():
    cursor_w = buffer_cursor()
    cursor_r = buffer_cursor()
    cursor_w.write_bytes_a(b'00000')
    cursor_w.write_bytes_a(b'00001')
    cursor_w.write_bytes_a(b'00002')
    cursor_r.buffer = cursor_w.buffer
    print(cursor_r.read_bytes())
    print(cursor_r.read_bytes())
    print(cursor_r.read_bytes())


# ─────────────────────────────────────────────
#  file backend 테스트
# ─────────────────────────────────────────────

def test_file_backend_int64_varchar():
    """write 후 파일을 닫고 새 커서로 다시 열어 값이 유지되는지 확인."""
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        # 쓰기
        with buffer_cursor.from_file(path, size=4096) as bc:
            bc.write_int64(123456789)
            bc.write_varchar("hello_file")
            bc.write_int64(987654321)

        # 파일을 닫고 새 커서로 읽기
        with buffer_cursor.from_file(path) as bc:
            assert bc.read_int64() == 123456789
            assert bc.read_varchar() == "hello_file"
            assert bc.read_int64() == 987654321
    finally:
        os.unlink(path)


def test_file_backend_bool_bytes():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        with buffer_cursor.from_file(path, size=4096) as bc:
            bc.write_bool(True)
            bc.write_bool(False)
            bc.write_bytes(b'\xde\xad\xbe\xef')
            bc.write_bytes(None)          # None → 길이 0으로 직렬화

        with buffer_cursor.from_file(path) as bc:
            assert bc.read_bool() 
            assert not bc.read_bool() 
            assert bc.read_bytes() == b'\xde\xad\xbe\xef'
            assert bc.read_bytes() is None
    finally:
        os.unlink(path)

def test_file_backend_resize_and_seek():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        path = f.name
    try:
        PAGE = 4096

        with buffer_cursor.from_file(path, size=PAGE) as bc:
            bc.at(0)
            bc.write_int64(11)

            bc.resize(PAGE * 2)
            bc.at(PAGE)
            bc.write_int64(22)

        with buffer_cursor.from_file(path) as bc:
            bc.at(0)
            assert bc.read_int64() == 11

            bc.at(PAGE)
            assert bc.read_int64() == 22
    finally:
        os.unlink(path)


if __name__ == "__main__":
    test_buffer_cursor_bytes()
    test_buffer_cursor_rwint64()
    test_file_backend_int64_varchar()
    test_file_backend_bool_bytes()
    test_file_backend_resize_and_seek()
    print("all tests passed.")