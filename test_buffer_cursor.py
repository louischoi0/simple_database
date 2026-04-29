from utils.buffer_cursor import buffer_cursor


def test_buffer_cursor_rwint64():
    cursor_w = buffer_cursor()
    cursor_r = buffer_cursor()

    str0 = "thisisname"
    print(str0.encode("utf-8"))

    num0 = 0

    cursor_w.write_varchar_a(str0)
    cursor_w.write_int64_a(num0)

    print(cursor_w.buffer)

    cursor_r.buffer = cursor_w.buffer

    assert cursor_r.read_varchar() == str0
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