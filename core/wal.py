import threading
from utils.buffer_cursor import buffer_cursor
from utils.payload_codec import payload_codec
from utils.logging import info
from core.meta import get_metablock

_info = lambda x: info("wal", x)

global g_xlog_writer
g_xlog_writer = None

global g_xlog_checkpointer
g_xlog_checkpointer = None

LSN_GENERATOR_LOCK = threading.Lock()

CMD_SIZE = 8
LSN_SIZE = 16

WAL_BUFFER_RING_SIZE = 1024 * 8
WAL_BUFFER_RING = bytearray(b"\0" * WAL_BUFFER_RING_SIZE)
global WAL_BUFFER_RING_OFFSET
WAL_BUFFER_RING_OFFSET = 0

WAL_SEGMENT_SIZE = 1024 * 8 * 8
WAL_BUFFER_OFFSET_LOCK = threading.Lock()

WAL_SEGMENT_LOCK = threading.Lock()
WAL_CHECKPOINTER_LOCK = threading.Lock()

WAL_SEGMENT_HEADER_SIZE = 0

def get_next_wal_seg_file(filename):
    num = int(filename[6:]) + 1
    return "walseg" + num.zfill(8)

class XLogWriter:
    WAL_SEGMENT_HDR_SIZE = 128

    def __init__(self):
        self.queue = []
        self.queue_lock = threading.Lock()

        self.current_wal_seg_file = get_metablock().get_value("current_wal_seg_file")
        self.last_lsn = None
        self.cursor_pos = WAL_SEGMENT_HEADER_SIZE
    
    def recovery_if_needed(self):
        pass

    def init(self):
        self.recovery_if_needed()
        offset = get_metablock().get_value("last_committed_wal_seg_pos")
        # TODO check there is uncommitted xlog over seg pos
        self.cursor_pos = offset
        self.last_lsn = get_metablock().get_value("checkpointer_lsn_committed")
    
    def enqueue_xlog(self, xlog):
        _info(f"enqueue xlog, cmd={xlog.cmd}; lsn={xlog.lsn}")
        with self.queue_lock:
            self.queue.append(xlog)
    
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
            self.last_lsn += 1
            return self.last_lsn
    
    def wal_seg_file_overflowed(self, size):
        return self.cursor_pos + size > WAL_SEGMENT_SIZE

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
    
    def create_new_wal_seg_file(self):
        self.cursor_pos = WAL_SEGMENT_HEADER_SIZE
        self.current_wal_seg_file = get_next_wal_seg_file(self.current_wal_seg_file)
        get_metablock().set_current_wal_seg_file_with_commit(self.current_wal_seg_file)

    def write_xlog(self, xlog):
        """
        (ring_buffer_cursor_start, ring_buffer_cursor_end,) , lsn = self.write_wal_entry(xlog.size())

        data = xlog.ser()
        length = len(data)

        assert ring_buffer_cursor_start + length == ring_buffer_cursor_end

        if ring_buffer_cursor_start > ring_buffer_cursor_end:
            raise Exception("not implemented")
        else:
            self.write_wal_ring_buffer(ring_buffer_cursor_start, data, length)
        """
        size = xlog.size()

        if self.wal_seg_file_overflowed(size):
            self.create_new_wal_seg_file()

        (_, _,) , lsn = self.write_wal_entry(size)
        xlog.set_lsn(lsn)
        self.enqueue_xlog(xlog)

    def consume_xlog_queue(self):
        while True:
            xlog = self.pop_xlog()
            if xlog is None:
                break
            self.write_xlog_into_segment_file(xlog)
    
    def write_xlog_into_segment_file(self, xlog):
        with WAL_SEGMENT_LOCK:
            with buffer_cursor.from_file(self.current_wal_seg_file, WAL_SEGMENT_SIZE) as cursor:
                buffer = xlog.ser()
                cursor.at(self.cursor_pos)
                cursor.write_raw(buffer)
                _info(f"walwriter pop xlog, xid={xlog.xid}; cmd={xlog.cmd}; page={xlog.payload.page_id} lsn={xlog.lsn}; length={len(buffer)}; start={self.cursor_pos}")
                self.set_cursor_pos(cursor)
    
    def set_cursor_pos(self, cursor):
        assert cursor.c <= WAL_SEGMENT_SIZE
        self.cursor_pos = cursor.c

    def proc(self):
        _info(f"xlog checkpointer process started")
        while True:
            self.consume_xlog_queue()
            from time import sleep
            sleep(1)

class XLogCheckpointer:

    def __init__(self, blk, begin, end, committed):
        self.begin_lsn = begin
        self.end_lsn = end
        self.committed_lsn = committed
        self.cursor_pos = WAL_SEGMENT_HEADER_SIZE
        self.blk = blk
        self.meta = get_metablock()
        self.current_wal_seg_file = self.meta.get_value("current_wal_seg_file")
        self.previous_lsn = committed

    def init(self):
        # TODO same as wal writer
        self.cursor_pos = get_metablock().get_value("last_committed_wal_seg_pos")

    def iter_xlog(self):
        with WAL_SEGMENT_LOCK:
            with buffer_cursor.from_file(self.current_wal_seg_file, WAL_SEGMENT_SIZE) as cursor:
                while True:
                    cursor.at(self.cursor_pos)

                    xlog_buffer, length = cursor.read_raw()

                    if xlog_buffer is None or len(xlog_buffer) == 0:
                        return

                    assert len(xlog_buffer) == length

                    xlog = XLog.decode(xlog_buffer)
                    _info(f"begin xlog commit lsn={xlog.lsn} len={len(xlog_buffer)} cursor_pos={self.cursor_pos}")
                    self.meta.set_begin_lsn_with_commit(xlog.lsn)
                    #assert xlog.lsn == self.previous_lsn + 1

                    self.do_commit_xlog(xlog)
                    self.set_current_cursor_pos(cursor)
                    self.meta.set_last_committed_wal_seg_pos_with_commit(self.cursor_pos)
                    self.previous_lsn = xlog.lsn

    def set_current_cursor_pos(self, cursor):
        self.cursor_pos = cursor.c

    def commit_page(self, pg):
        self.blk.write_page(pg)
        pg.clear_dirty_flag()
    
    def do_commit_xlog(self, xlog):
        if xlog.cmd == "hinsertx":
            from core.page_mgr import ref_heap_page
            page = ref_heap_page(xlog.payload.page_id)
            self.commit_page(page)

        self.meta.set_end_lsn_with_commit(xlog.lsn)
        self.meta.set_commit_lsn_with_commit(xlog.lsn)

    def proc(self):
        while True:
            self.iter_xlog()
            from time import sleep
            sleep(1)

    def wait_to_terminate(self):
        pass

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
        size = cursor.read_int64()

        assert len(buffer) == size

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

def create_xlog_heap_insert_cmd(xid, page_id, slot_index, tuple):
    return XLogHeapInsertCMD(xid, XLogHeapInsertPayload(page_id, slot_index, tuple.ser()))

class XLogHeapInsertPayload:
    def __init__(self, page_id, slot_index, tuple_buffer):
        self.page_id = page_id
        self.slot_index = slot_index
        self.tuple_buffer = tuple_buffer
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(self.page_id)
        cursor.write_int64_a(self.slot_index)
        cursor.write_bytes_a(self.tuple_buffer)
        return cursor.buffer

    @classmethod
    def decode(cls, buffer):
        cursor = buffer_cursor(buffer)

        page_id = cursor.read_int64()
        slot_index = cursor.read_int64()
        buffer = cursor.read_bytes()

        return XLogHeapInsertPayload(page_id, slot_index, buffer)

class XLogBtreeInsertSlotCMDPayload:
    def __init__(self, target_page_id, new_page_id, slot_index):
        self.target_page_id = target_page_id
        self.new_page_id = new_page_id
        self.slot_index = slot_index
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(self.target_page_id)
        cursor.write_int64_a(self.new_page_id)
        cursor.write_int64_a(self.slot_index)

    @classmethod
    def decode(cls, buffer):
        cursor = buffer_cursor(buffer)

        target_page_id = cursor.read_int64()
        new_page_id = cursor.read_int64()
        slot_index = cursor.read_int64()

        return XLogBtreeInsertSlotCMDPayload(target_page_id, new_page_id, slot_index)

class XLogBtreeInsertSlotCMD(XLog):
    def __init__(self, xid, payload: XLogBtreeInsertSlotCMDPayload):
        super(XLogBtreeInsertSlotCMD, self).__init__(xid, "binserth", payload)

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

def _init_wal_system(blk, metablock):
    global g_xlog_writer
    g_xlog_writer = XLogWriter()
    g_xlog_writer.init()

    global g_xlog_checkpointer
    g_xlog_checkpointer = XLogCheckpointer(
        blk, 
        begin=metablock.checkpointer_lsn_begin, 
        end=metablock.checkpointer_lsn_end, 
        committed=metablock.checkpointer_lsn_committed
    )

    g_xlog_checkpointer.init()

    return g_xlog_writer, g_xlog_checkpointer

def global_write_xlog(xlog):
    global g_xlog_writer
    return g_xlog_writer.write_xlog(xlog)