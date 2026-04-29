import threading
from utils.buffer_cursor import buffer_cursor
from utils.payload_codec import payload_codec

LSN_GENERATOR_LOCK = threading.Lock()
LAST_LSN = 0
CMD_SIZE = 8

WAL_BUFFER_RING_SIZE = 1024 * 8
WAL_BUFFER_RING = b"\0" * WAL_BUFFER_RING_SIZE
WAL_BUFFER_OFFSET = 0
WAL_BUFFER_OFFSET_LOCK = threading.Lock()

def generate_lsn():
    with LSN_GENERATOR_LOCK:
        LAST_LSN += 1
        return LAST_LSN

class XLog:
    def __init__(self, lsn, cmd, ref_oid, payload):
        self.lsn = lsn
        self.cmd = cmd
        self.ref_oid = ref_oid
        self.payload = payload
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(self.lsn)

        assert len(self.cmd) 

        cursor.write_varchar_a(self.cmd)
        cursor.write_int64_a(self.ref_oid)

        payload_buffer = payload_codec.encode(self.payload)
        cursor.write_bytes_a(payload_buffer)

        return cursor.buffer
    
    @classmethod
    def decode(self, buffer):
        cursor = buffer_cursor(buffer)
        lsn = cursor.read_int64()
        cmd = cursor.read_varchar()
        ref_oid = cursor.read_int64()
        payload = payload_codec.decode(cursor.tail())

        return XLog(lsn, cmd, ref_oid, payload)

class XLogInsertCMD(XLog):
    def __init__(self, lsn, ref_oid, payload):
        cmd = "insertxx"
        super(XLogInsertCMD, self).__init__(lsn, cmd, ref_oid, payload)

def inc_wal_buffer_offset(size):
    WAL_BUFFER_OFFSET_LOCK.acquire()
    start = WAL_BUFFER_OFFSET
    end = (WAL_BUFFER_OFFSET + size) % WAL_BUFFER_RING_SIZE
    WAL_BUFFER_OFFSET = end
    WAL_BUFFER_OFFSET_LOCK.release()
    return start, end

def write_wal_buffer(xlog):
    data = xlog.ser()
    length = data

    start, end = inc_wal_buffer_offset(length+8)

    if start > end:
        raise Exception("wip")
    else:
        buffer = WAL_BUFFER_RING[start: end]
        cursor = buffer_cursor(buffer)
        cursor.write_int64(length)
        cursor.write_bytes(data)
        WAL_BUFFER_RING[start: end]  = cursor.buffer