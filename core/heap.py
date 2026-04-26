from core.const import *
from core.page import page
from utils.buffer_cursor import buffer_cursor
from utils.dec import *

class heap_tuple:
    def __init__(self, value, lsn=None, tid=None):
        self.value = value
        self.lsn = lsn
    
    def size(self):
        return 8
    
    def ser(self):
        return serint64(self.value)
    
    @classmethod
    def parse(self, buffer):
        return toint64(buffer)

class heap_page(page):
    def __init__(self, page_id):
        super().__init__(page_id, PAGE_TYPE_HEAP, -1)
        self.tuple_count = 0
    
    def initial_insert(self, t):
        self.insert(t)
        self.min_key = t.value
    
    def insert(self, t):
        assert t.value >= self.min_key

        self.buffer[HDR_SIZE:HDR_SIZE+8] = serint64(self.tuple_count)
        data_offset = HDR_SIZE + (self.tuple_count * 8) + 8

        assert len(t.ser()) == 8
        self.buffer[data_offset:data_offset+t.size()] = t.ser()

        assert t.value == heap_tuple.parse(t.ser())
        self.tuple_count += 1
    
    def ptype(self):
        return "heap"
    
    def iter(self, f):
        read_cursor = buffer_cursor(self.buffer, self.id)
        read_cursor.advance(HDR_SIZE)
        _tuple_count = read_cursor.read_int64()

        assert _tuple_count == self.tuple_count

        for _ in range(self.tuple_count):
            value = read_cursor.read_int64()
            f(value)

    def update_header_buffer(self):
        header_buffer = self.ser_header()

        assert len(header_buffer) == HDR_SIZE + 8

        self.buffer[:len(header_buffer)] = header_buffer
        self.mark_dirty_flag()

    def ser_header(self):
        return (
            serint64(self.id) +
            serint64(self.type) +
            serint64(self.min_key) + 
            serint64(self.tuple_count)
        )
    
    @classmethod
    def parse_header_buffer(cls, buffer):
        return (
            toint64(buffer[0:8]),
            toint64(buffer[8:16]),
            toint64(buffer[16:24]),
            toint64(buffer[24:32]),
        )

    def apply_header_buffer(self):
        id, type, min_key, tuple_count = self.parse_header_buffer(self.buffer)
        c = buffer_cursor(self.buffer, id)
        c.advance(HDR_SIZE)

        self.id = id
        self.type = type
        self.min_key = min_key
        self.tuple_count = tuple_count
