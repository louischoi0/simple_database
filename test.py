from utils.payload_codec import payload_codec
from core.wal import XLogHeapInsertCMD, write_xlog, XLog, _init_wal_system


def test1():
    data = { "a": "asdfsdafb", "c": "1111"}
    buffer = payload_codec.encode(data)
    data2 = payload_codec.decode(buffer)
    print(data)
    print(data2)
  
def test2():
    payload = { "a": "asdfsdafb", "c": "1111"}
    cmd = XLogHeapInsertCMD(0, 1, payload)

    buffer = cmd.ser()

    cmd2 = XLog.decode(buffer)

    assert cmd.cmd == cmd2.cmd

def test3():
    payload = { "a": "asdfsdafb", "c": "1111"}
    cmd = XLogHeapInsertCMD(0, 1, payload)

    write_xlog(cmd)

if __name__ == "__main__":
    _init_wal_system()
    test3()