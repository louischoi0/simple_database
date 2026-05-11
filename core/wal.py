import threading
from utils.buffer_cursor import buffer_cursor
from utils.payload_codec import payload_codec
from utils.logging import info

_info = lambda x: info("wal", x)

global g_xlog_writer
g_xlog_writer = None

global g_xlog_checkpointer
g_xlog_checkpointer = None

global LAST_LSN

LSN_GENERATOR_LOCK = threading.Lock()
LAST_LSN = 0

CMD_SIZE = 8
LSN_SIZE = 16

WAL_BUFFER_RING_SIZE = 1024 * 8
WAL_BUFFER_RING = bytearray(b"\0" * WAL_BUFFER_RING_SIZE)
global WAL_BUFFER_RING_OFFSET
WAL_BUFFER_RING_OFFSET = 0

WAL_SEGMENT_SIZE = 1024 * 8 * 8
WAL_BUFFER_OFFSET_LOCK = threading.Lock()

WAL_SEGMENT_LOCK = threading.Lock()
CURRENT_SEGMENT_FILE = "walseg00000000"

class XLogWriter:
    WAL_SEGMENT_HDR_SIZE = 128

    def __init__(self):
        self.queue = []
        self.queue_lock = threading.Lock()
        self.current_segment_file = CURRENT_SEGMENT_FILE

    def enqueue_xlog(self, xlog):
        _info(f"enqueue xlog, cmd={xlog.cmd}; lsn={xlog.lsn}")

        self.queue_lock.acquire()
        self.queue.append(xlog)
        self.queue_lock.release()
    
    def pop_xlog(self):
        with self.queue_lock:
            if len(self.queue) == 0:
                return None

            item = self.queue[0]
            self.queue = self.queue[1:]
            return item

    def inc_wal_segment_id(self):
        WAL_SEGMENT_CURSOR += 1

    def write_wal_segment_entry(self, size):
        with LSN_GENERATOR_LOCK:
            global LAST_LSN
            LAST_LSN += 1
            return LAST_LSN

    def write_wal_ring_buffer_entry(self, size):
        global WAL_BUFFER_RING_OFFSET
        start = WAL_BUFFER_RING_OFFSET
        WAL_BUFFER_RING_OFFSET = (WAL_BUFFER_RING_OFFSET + size) % WAL_BUFFER_RING_SIZE
        return start, WAL_BUFFER_RING_OFFSET

    def write_wal_ring_buffer(self, offset, data, size):
        buffer = WAL_BUFFER_RING[offset: offset + size]

        cursor = buffer_cursor(buffer)
        cursor.write_raw(data)

        WAL_BUFFER_RING[offset: offset+size]  = cursor.buffer

    def write_wal_entry(self, size):
        WAL_BUFFER_OFFSET_LOCK.acquire()
        ring_offset, ring_offset_end = self.write_wal_ring_buffer_entry(size)
        lsn = self.write_wal_segment_entry(size)

        WAL_BUFFER_OFFSET_LOCK.release()

        return (ring_offset, ring_offset_end), lsn

    def write_xlog(self, xlog):

        (ring_buffer_cursor_start, ring_buffer_cursor_end,) , lsn = self.write_wal_entry(xlog.size())
        xlog.set_lsn(lsn)

        data = xlog.ser()
        length = len(data)

        assert ring_buffer_cursor_start + length == ring_buffer_cursor_end

        if ring_buffer_cursor_start > ring_buffer_cursor_end:
            raise Exception("not implemented")
        else:
            self.write_wal_ring_buffer(ring_buffer_cursor_start, data, length)
        
        self.enqueue_xlog(xlog)

    def consume_xlog_queue(self):
        while True:
            xlog = self.pop_xlog()
            if xlog is None:
                break
            self.write_xlog_into_segment_file(xlog)
    
    def write_xlog_into_segment_file(self, xlog):
        with WAL_SEGMENT_LOCK:
            with buffer_cursor.from_file(self.current_segment_file, WAL_SEGMENT_SIZE) as cursor:
                buffer = xlog.ser()
                cursor.write_raw(buffer)
                _info(f"xlog walwriter pop xlog, cmd={xlog.cmd}; lsn={xlog.lsn}; length={len(buffer)}")

    def proc(self):
        _info(f"xlog checkpointer process started")
        while True:
            self.consume_xlog_queue()
            from time import sleep
            sleep(1)

class XLogCheckpointer:

    def __init__(self):
        self.checkpoint_lsn = 0
        self.current_segment_file = CURRENT_SEGMENT_FILE

    def iter_xlog(self):
        with WAL_SEGMENT_LOCK:
            with buffer_cursor.from_file(self.current_segment_file, WAL_SEGMENT_SIZE) as cursor:
                xlog_buffer = cursor.read_bytes()
                if xlog_buffer is None or len(xlog_buffer) == 0:
                    return
                xlog = XLog.decode(xlog_buffer)

    def proc(self):
        while True:
            self.iter_xlog()
            from time import sleep
            sleep(1)

class XLog:
    def __init__(self, xid, cmd, payload, lsn=None, prev_lsn=None):
        self.xid = xid
        self.cmd = cmd
        self.prev_lsn = prev_lsn
        self.lsn = lsn
        self.payload = payload
    
    def set_lsn(self, lsn):
        self.lsn = lsn
    
    def set_prev_lsn(self, prev_lsn):
        self.prev_lsn = prev_lsn
    
    def size(self):
        return len(self.ser())
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(0)
        cursor.write_int64_a(self.lsn or 0)
        cursor.write_int64_a(self.prev_lsn or 0)
        cursor.write_int64_a(self.xid)
        cursor.write_varchar_a(self.cmd)

        #payload_buffer = payload_codec.encode(self.payload)
        payload_buffer = self.payload.ser()
        cursor.write_raw_a(payload_buffer)

        cursor.at(0)
        cursor.write_int64(len(cursor.buffer))

        return cursor.buffer
    
    @classmethod
    def decode(cls, buffer):
        cursor = buffer_cursor(buffer)

        lsn = cursor.read_int64()
        prev_lsn = cursor.read_int64()
        xid = cursor.read_int64()
        cmd = cursor.read_varchar()

        #payload = payload_codec.decode(cursor.tail())
        payload = cls.parse_xlog_payload(cmd, cursor.tail())

        return XLog(xid=xid, cmd=cmd, payload=payload, lsn=lsn, prev_lsn=prev_lsn)

    @classmethod
    def parse_xlog_payload(cls, cmd, payload_buffer):
        if cmd == "hinsertx":
            return XLogHeapInsertPayload.decode(payload_buffer)
        else:
            raise Exception(f"unknown xlog cmd type: {cmd}")

def create_xlog_heap_insert_cmd(xid, rel_id, page_id, slot_index, tuple):
    return XLogHeapInsertCMD(xid, XLogHeapInsertPayload(rel_id, page_id, slot_index, tuple.ser()))

class XLogHeapInsertPayload:
    def __init__(self, rel_id, page_id, slot_index, tuple_buffer):
        self.rel_id = rel_id
        self.page_id = page_id
        self.slot_index = slot_index
        self.tuple_buffer = tuple_buffer
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(self.rel_id)
        cursor.write_int64_a(self.page_id)
        cursor.write_int64_a(self.slot_index)
        cursor.write_bytes_a(self.tuple_buffer)
        return cursor.buffer

    @classmethod
    def decode(cls, buffer):
        cursor = buffer_cursor(buffer)

        rel_id = cursor.read_int64()
        page_id = cursor.read_int64()
        slot_index = cursor.read_int64()
        buffer = cursor.read_bytes()

        return XLogHeapInsertPayload(rel_id, page_id, slot_index, buffer)

class XLogHeapInsertCMD(XLog):
    def __init__(self, xid, payload: XLogHeapInsertPayload):
        super(XLogHeapInsertCMD, self).__init__(xid, "hinsertx", payload)

class XLogBeginTransactionPayload:
    def __init__(self, xid):
        self.xid = xid
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(self.xid)
        return cursor.buffer

    @classmethod 
    def decode(cls, buffer):
        cursor = buffer_cursor(buffer)
        xid = cursor.read_int64()
        return XLogBeginTransactionPayload(xid)


class XLogBeginTransactionCMD(XLog):
    def __init__(self, xid):
        super(XLogBeginTransactionCMD, self).__init__(xid, "0begintx", XLogBeginTransactionPayload(self.xid))


def _init_wal_system():
    global g_xlog_writer
    g_xlog_writer = XLogWriter()

    global g_xlog_checkpointer
    g_xlog_checkpointer = XLogCheckpointer()

    return g_xlog_writer, g_xlog_checkpointer