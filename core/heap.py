from core.const import *
from core.page import page
from utils.buffer_cursor import buffer_cursor
from utils.dec import *

HEAP_TUPLE_HEADER_SIZE = 24 # size xmin, xmax

class TupleVersion:
    def __init__(self, xmin, xmax):
        self.xmin = xmin
        self.xmax = xmax

class HeapTuple:
    def __init__(self, buffer: bytearray):
        self.buffer: bytearray = buffer
        cursor = buffer_cursor(buffer)
        self.size = cursor.read_int64()
        xmin, xmax = cursor.read_int64(), cursor.read_int64()
        self.version = TupleVersion(xmin, xmax)
    
    def create(self, data_buffer, version=None):
        cursor = buffer_cursor() 
        heap_tuple_size = len(data_buffer) + HEAP_TUPLE_HEADER_SIZE
        
        cursor.write_int64_a(heap_tuple_size)

        if self.version is None:
            cursor.pad_a(HEAP_TUPLE_HEADER_SIZE)
        else:
            cursor.write_int64_a(version.xmin)
            cursor.write_int64_a(version.xmax)

        cursor.write_bytes_a(data_buffer)
        return HeapTuple(cursor.buffer)
    
    def ser(self):
        return self.buffer
    
    @classmethod
    def parse(self, buffer):
        return toint64(buffer)

class StructuredTuple(HeapTuple):
    def __init__(self, buffer):
        super(StructuredTuple, self).__init__(buffer)
        self.struct()

    def get(self, key):
        return self.structured_data[key]
    
    def ser_data(self):
        pass

    def parse_data(self):
        pass

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
    def load(self, schema, dictionary, version=None):
        cursor = buffer_cursor()
        cursor.pad_a(HEAP_TUPLE_HEADER_SIZE)

        for idx, c in enumerate(schema):
            assert c.pos == idx
            value = dictionary[c.name]

            cursor.write_dynamic_type_a(c.type.value, value)

        t = StructuredTuple(schema, cursor.buffer, version=version)
        t.structured_data = dictionary
        return t

class heap_page(page):

    SLOT_SIZE = 4
    SLOT_SEGMENT_OFFSET = HDR_SIZE + 8 + 8
    TUPLE_SEGMENT_OFFSET = PAGE_SIZE - 16
    TAIL_SEGMENT_SIZE = 32

    def __init__(self, page_id):
        super().__init__(page_id, PAGE_TYPE_HEAP, -1)
        self.tuple_count = 0
        self.slot_cursor = PAGE_SIZE - heap_page.TAIL_SEGMENT_SIZE
        self.slots = []
    
    def write_tuple_count(self):
        self.buffer[HDR_SIZE:HDR_SIZE+8] = serint64(self.tuple_count)

    def add_slot(self):
        if self.slot_cursor < heap_page.SLOT_SEGMENT_OFFSET:
            raise Exception(f"heap page overflow error: tried to write tuple data at pos:{self.slot_cursor}")

        slot_buffer_offset = heap_page.SLOT_SEGMENT_OFFSET + (self.tuple_count * self.SLOT_SIZE)
        self.buffer[slot_buffer_offset: slot_buffer_offset + self.SLOT_SIZE] = serint32(self.slot_cursor)
        self.slots.append(self.slot_cursor)
    
    def raw_iter(self, f):
        cursor = self.cursor()

        for tuple_pos in self.slot_cursor:
            cursor.at(tuple_pos)

            xmin = cursor.read_int64()
            xmax = cursor.read_int64()
            data = cursor.read_bytes()

            f(xmin, xmax, data)
    
    def write_tuple_data(self, size, data_buffer):
        # insert write only page buffer
        # todo: write to wal segment for fist instead page buffer directly

        self.buffer[self.slot_cursor: self.slot_cursor+size] = data_buffer
        self.slot_cursor += size

    def insert(self, t):
        self.acquire_lock()

        if self.tuple_count == 0:
            self.min_key = t.key
        else:
            assert t.key >= self.min_key

        self.tuple_count += 1

        data_buffer = t.ser()
        data_size = t.size()

        self.write_tuple_count()
        self.slot_cursor -= data_size
        self.add_slot()
        self.write_tuple_data(data_size, data_buffer)

        self.release_lock()
    
    def ptype(self):
        return "heap"
    
    def iter(self, f):
        pass
 
    def update_header_buffer(self):
        header_buffer = self.ser_header()

        assert len(header_buffer) == HDR_SIZE + 8

        self.buffer[:len(header_buffer)] = header_buffer
        self.mark_dirty_flag()
    
    def update_slot_buffer(self):
        self.mark_dirty_flag()
    
    def update_data_buffer(self):
        self.mark_dirty_flag()

    def ser_header(self):
        return (
            serint64(self.key) +
            serint64(self.type) +
            serint64(self.min_key) + 
            serint64(self.tuple_count) + 
            serint64(self.slot_cursor)
        )
    
    @classmethod
    def parse_header_buffer(cls, buffer):
        return (
            toint64(buffer[0:8]),
            toint64(buffer[8:16]),
            toint64(buffer[16:24]),
            toint64(buffer[24:32]),
            toint64(buffer[24:40]),
        )

    def apply_header_buffer(self):
        key, type, min_key, tuple_count, slot_cursor = self.parse_header_buffer(self.buffer)
        c = buffer_cursor(self.buffer, key)
        c.advance(HDR_SIZE)

        self.key = key
        self.type = type
        self.min_key = min_key
        self.tuple_count = tuple_count

        self.slot_cursor = slot_cursor