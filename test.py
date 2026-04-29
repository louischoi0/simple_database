from utils.payload_codec import payload_codec
from core.wal import XLogInsertCMD, write_wal_buffer, XLog


def test1():
    data = { "a": "asdfsdafb", "c": "1111"}
    buffer = payload_codec.encode(data)
    data2 = payload_codec.decode(buffer)
    print(data)
    print(data2)
  
def test2():
    payload = { "a": "asdfsdafb", "c": "1111"}
    cmd = XLogInsertCMD(0, 1, payload)

    buffer = cmd.ser()

    cmd2 = XLog.decode(buffer)

    assert cmd.cmd == cmd2.cmd

if __name__ == "__main__":
    test2()