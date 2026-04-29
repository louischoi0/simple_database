from core.const import *
from core.page import page
from utils.buffer_cursor import buffer_cursor
from utils.dec import *

HEAP_TUPLE_HEADER_SIZE = 16

class TupleVersion:
    def __init__(self, xmin, xmax):
        self.xmin = xmin
        self.xmax = xmax

class HeapTuple:
    def __init__(self, buffer, version=None):
        self.buffer: bytearray = buffer
        self.version = version
    
    def size(self):
        # xmin, xmax
        return  HEAP_TUPLE_HEADER_SIZE + len(self.buffer)
    
    def ser(self):
        cursor = buffer_cursor()
        cursor.write_int64_a(self.size())

        if self.version is None:
            cursor.pad_a(HEAP_TUPLE_HEADER_SIZE)
        else:
            cursor.write_int64_a(self.version.xmin)
            cursor.write_int64_a(self.version.xmax)

        cursor.write_varchar(self.buffer)
        return cursor.buffer
    
    @classmethod
    def parse(self, buffer):
        return toint64(buffer)
    
    @property
    def key(self):
        cursor = self.buffer(self.buffer)
        cursor.advance(HEAP_TUPLE_HEADER_SIZE)

        return cursor.read_int64()

class StructuredTuple(HeapTuple):
    def __init__(self, schema, buffer, version=None):
        super(StructuredTuple, self).__init__(buffer, version)
        self.schema = schema
        self.structured_data = {}
        self.struct()
    
    def get(self, key):
        return self.structured_data[key]

    def struct(self):
        cursor = buffer_cursor(self.buffer)
        cursor.advance(HEAP_TUPLE_HEADER_SIZE)

        for idx, c in enumerate(self.schema):
            assert c.pos == idx
            value = cursor.read_dynamic_type_a(c.type.value)
            self.structured_data[c.name] = value
        return self.structured_data
    
    @classmethod
    def parse(self, schema, buffer, version=None):
        t = StructuredTuple(schema, buffer, version)
        t.struct()
        return t
    
    @classmethod
    def load(self, schema, dictionary):
        cursor = buffer_cursor()
        cursor.pad_a(HEAP_TUPLE_HEADER_SIZE)

        for idx, c in enumerate(schema):
            assert c.pos == idx
            value = dictionary[c.name]

            cursor.write_dynamic_type_a(c.type.value, value)

        t = StructuredTuple(schema, cursor.buffer)
        t.structured_data = dictionary
        return t


class heap_page(page):
    def __init__(self, page_id):
        super().__init__(page_id, PAGE_TYPE_HEAP, -1)
        self.tuple_count = 0
    
    def initial_insert(self, t):
        self.insert(t)
        self.min_key = t.key
    
    def insert(self, t):
        assert t.key>= self.min_key
        self.acquire_lock()

        self.buffer[HDR_SIZE:HDR_SIZE+8] = serint64(self.tuple_count)
        data_offset = HDR_SIZE + (self.tuple_count * 8) + 8

        assert len(t.ser()) == 8
        self.buffer[data_offset:data_offset+t.size()] = t.ser()

        self.tuple_count += 1
        self.release_lock()
    
    def ptype(self):
        return "heap"
    
    def iter(self, f):
        read_cursor = buffer_cursor(self.buffer, self.key)
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
            serint64(self.key) +
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
        key, type, min_key, tuple_count = self.parse_header_buffer(self.buffer)
        c = buffer_cursor(self.buffer, key)
        c.advance(HDR_SIZE)

        self.key = key
        self.type = type
        self.min_key = min_key
        self.tuple_count = tuple_count
