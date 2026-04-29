import threading
from utils.buffer_cursor import buffer_cursor
from utils.payload_codec import payload_codec
from utils.logging import info

global g_xlog_writer
g_xlog_writer = None

LSN_GENERATOR_LOCK = threading.Lock()
LAST_LSN = 0
CMD_SIZE = 8
LSN_SIZE = 16

WAL_BUFFER_RING_SIZE = 1024 * 8
WAL_BUFFER_RING = b"\0" * WAL_BUFFER_RING_SIZE
WAL_BUFFER_RING_OFFSET = 0

WAL_SEGMENT_SIZE = 1024 * 8 * 8
WAL_SEGMENT_CURSOR = 0
WAL_SEGMENT_OFFSET = 0

WAL_BUFFER_OFFSET_LOCK = threading.Lock()

def generate_lsn():
    with LSN_GENERATOR_LOCK:
        LAST_LSN += 1
        return LAST_LSN

class LSN:
    def __init__(self, wal_segment, offset):
        self.wal_segment = wal_segment
        self.offset = offset
        self.lsn = LSN.encode(wal_segment, offset)
    
    @classmethod
    def decode(self, lsn):
        cursor = buffer_cursor(lsn)
        
        wal_segment = cursor.read_char(4)
        offset = cursor.read_int64()
        return LSN(lsn, wal_segment, offset)

    @classmethod
    def encode(self, wal_segment, offset):
        cursor = buffer_cursor()
        cursor.write_char_a(wal_segment)
        cursor.write_int64_a(offset)
        return cursor.buffer

class XLogWriter:
    def __init__(self):
        self.queue = []
        self.queue_lock = threading.Lock()

    def enqueue_xlog(self, xlog):
        self.queue_lock.acquire()
        self.queue.append(xlog)
        self.queue_lock.release()

def _init_wal_system():
    global g_xlog_writer
    g_xlog_writer = XLogWriter()

class XLog:
    def __init__(self, xid, cmd, payload):
        self.lsn = None
        self.xid = xid
        self.cmd = cmd
        self.payload = payload
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(self.lsn.lsn)
        cursor.write_int64_a(self.xid)

        assert len(self.cmd) 

        cursor.write_varchar_a(self.cmd)

        payload_buffer = payload_codec.encode(self.payload)
        cursor.write_bytes_a(payload_buffer)

        return cursor.buffer
    
    @classmethod
    def decode(self, buffer):
        cursor = buffer_cursor(buffer)
        lsn = cursor.read_int64()
        xid = cursor.read_int64()
        cmd = cursor.read_varchar()
        payload = payload_codec.decode(cursor.tail())

        return XLog(lsn, xid, cmd, payload)

class XLogHeapInsertCMD(XLog):
    def __init__(self, payload):
        cmd = "hinsertx"
        super(XLogHeapInsertCMD, self).__init__(cmd, payload)

def inc_wal_segment_id():
    WAL_SEGMENT_CURSOR += 1

def write_wal_segment_entry(size):
    if WAL_SEGMENT_SIZE < size + WAL_SEGMENT_OFFSET:
        inc_wal_segment_id()
        WAL_SEGMENT_OFFSET = 0
    else:
        WAL_SEGMENT_OFFSET += size

    return LSN(WAL_SEGMENT_CURSOR, WAL_SEGMENT_OFFSET)

def write_wal_ring_buffer_entry(size):
    start = WAL_BUFFER_RING_OFFSET
    WAL_BUFFER_RING_OFFSET = (WAL_BUFFER_RING_OFFSET + size) % WAL_BUFFER_RING_SIZE
    return start, WAL_BUFFER_RING_OFFSET

def write_wal_ring_buffer(offset, data, size):
    buffer = WAL_BUFFER_RING[offset: offset + size]

    cursor = buffer_cursor(buffer)
    cursor.write_bytes(data)

    WAL_BUFFER_RING[offset: offset+size]  = cursor.buffer

def write_wal_entry(size):

    WAL_BUFFER_OFFSET_LOCK.acquire()
    ring_offset, ring_offset_end = write_wal_ring_buffer_entry(size)
    lsn = write_wal_segment_entry(size)

    WAL_BUFFER_OFFSET_LOCK.release()

    return (ring_offset, ring_offset_end), lsn

def write_xlog(xlog):
    assert xlog.lsn is None

    data = xlog.ser()
    length = len(data)

    (ring_buffer_cursor_start, ring_buffer_cursor_end,) , lsn = write_wal_entry(length)
    xlog.lsn = lsn

    assert ring_buffer_cursor_start + length == ring_buffer_cursor_end

    if ring_buffer_cursor_start > ring_buffer_cursor_end:
        raise Exception("not implemented")
    else:
        write_wal_ring_buffer(ring_buffer_cursor_start, data, length)
    
    g_xlog_writer.enqueue_xlog(xlog)


