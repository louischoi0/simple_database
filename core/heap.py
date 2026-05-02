from core.const import *
from core.page import page
from utils.buffer_cursor import buffer_cursor
from utils.dec import *
from utils.logging import info

XFLAG_SIZE = 8
_info = lambda msg: info("heap", msg)

class TupleVersion:
    def __init__(self, xmin, xmax):
        self.xmin = xmin
        self.xmax = xmax

class HeapTuple:
    HEAP_TUPLE_HEADER_SIZE = 32 # size, xmin, xmax, reserved (null bit mask for structured heap tuple)
    HEAP_TUPLE_RESERVED_SIZE = 8

    def __init__(self, buffer: bytearray):
        self.buffer: bytearray = buffer
        cursor = buffer_cursor(buffer)
        self.size = cursor.read_int64()
        xmin, xmax = cursor.read_int64(), cursor.read_int64()
        self.version = TupleVersion(xmin, xmax)
    
    def ser(self):
        return self.buffer
    
    @classmethod
    def parse(self, buffer):
        return toint64(buffer)

class StructuredTuple(HeapTuple):
    def __init__(self, buffer):
        super(StructuredTuple, self).__init__(buffer)
        self.structured_data = {}

    def get(self, key):
        return self.structured_data[key]

    def struct(self, schema):
        cursor = buffer_cursor(self.buffer)
        cursor.advance(HeapTuple.HEAP_TUPLE_HEADER_SIZE)

        for idx, c in enumerate(schema.col_arr):
            assert c.pos == idx
            value = cursor.read_dynamic_type_a(c.type.value)
            self.structured_data[c.name] = value

        return self.structured_data
    
    def get_null_flag_buffer(self):
        cursor = buffer_cursor(self.buffer)
        cursor.at(HeapTuple.HEAP_TUPLE_HEADER_SIZE - 8)

        return cursor.read_int64()
    
    def get_null_flag(self, colnum):
        value = self.get_null_flag_buffer()
        return bool(value & (1 << colnum))
    
    def set_null_flag(self, colnum, flag):
        value = self.get_null_flag_buffer()
        value = (value & ~(1 << colnum)) | (int(flag) << colnum)

        cursor.at(HeapTuple.HEAP_TUPLE_HEADER_SIZE - 8)
        cursor.write_int64_a()

    @classmethod
    def parse(self, buffer):
        t = StructuredTuple(buffer)
        
        cursor = buffer_cursor(buffer)

        t.size = cursor.read_int64()
        t.xmin = cursor.read_int64()
        t.xmax = cursor.read_int64()
        t.reserved = cursor.read_int64()

        return t
    
    @classmethod
    def load(self, schema, dictionary, version=None) -> StructuredTuple: 
        cursor = buffer_cursor()
        cursor.pad_a(HeapTuple.HEAP_TUPLE_HEADER_SIZE)

        for idx, c in enumerate(schema.col_arr):
            assert c.pos == idx
            value = dictionary[c.name]

            cursor.write_dynamic_type_a(c.type.value, value)

        size = len(cursor.buffer)
        cursor.at(0) 
        cursor.write_int64(size)

        if version is None:
            cursor.pad(XFLAG_SIZE * 2)
        else:
            cursor.write_int64(version.xmin)
            cursor.write_int64(version.xmax)
        
        return StructuredTuple.parse(cursor.buffer)

class heap_page(page):
    SLOT_SIZE = 4
    HEAP_PAGE_HDR_SIZE = 40
    SLOT_SEGMENT_OFFSET = HEAP_PAGE_HDR_SIZE
    MIN_KEY_OFFSET = 16
    TAIL_SEGMENT_SIZE = 32
    TUPLE_SEGMENT_OFFSET = PAGE_SIZE - TAIL_SEGMENT_SIZE
    HEAP_PAGE_HDR_SIZE = HDR_SIZE + 16

    def __init__(self, page_id):
        super().__init__(page_id, PAGE_TYPE_HEAP, -1)
        self.tuple_count = 0
        self.slot_cursor = PAGE_SIZE - heap_page.TAIL_SEGMENT_SIZE
        self.slots = []
        self.deleted = []
        self.cursor = buffer_cursor(self.buffer)
    
    def write_tuple_count(self):
        _info(f"write tuple_count to heap page:{self.id} count={self.tuple_count}")
        self.buffer[HDR_SIZE:HDR_SIZE+8] = serint64(self.tuple_count)

    def add_slot(self, tuple_size):
        last = self.slot_cursor
        self.slot_cursor -= tuple_size

        if tuple_size < HeapTuple.HEAP_TUPLE_HEADER_SIZE:
            raise Exception(f"heap tuple size smaller than HEAP_TUPLE_HEADER_SIZE, {tuple_size}")

        if self.slot_cursor < heap_page.SLOT_SEGMENT_OFFSET:
            raise Exception(f"heap page overflow error: tried to write tuple data at pos:{self.slot_cursor}")

        slot_buffer_offset = heap_page.SLOT_SEGMENT_OFFSET + ((self.tuple_count - 1) * self.SLOT_SIZE)

        self.cursor.at(slot_buffer_offset)
        self.cursor.write_int32(self.slot_cursor)

        _info(f"add slot to heap page:{self.id} index={self.tuple_count-1}, offset={slot_buffer_offset}, value={self.slot_cursor}, tuple_size={last - self.slot_cursor}")
        self.slots.append(self.slot_cursor)
    
    def raw_map(self, f):
        cursor = self.cursor
        res = []

        for index, tuple_pos in enumerate(self.slots):
            if index in self.deleted:
                continue 

            cursor.at(tuple_pos)
            size = cursor.read_int64()

            assert size > 0
            cursor.at(tuple_pos)
            buffer = cursor.read(size)

            res.append(f(buffer))

        return res
    
    def load_slots_from_buffer(self):
        cursor = self.cursor
        cursor.at(heap_page.SLOT_SEGMENT_OFFSET)

        self.slots = []

        for _ in range(self.tuple_count):
            slot = cursor.read_int32()
            self.slots.append(slot)
    
    def activate(self):
        # after read page buffer from disk
        # activate function fill all vars of instance
        # check deleted tuples and put it self.deleted
        self.acquire_lock()
        self.apply_header_buffer()
        self.load_slots_from_buffer()
        self.deleted = []

        for i, pos in enumerate(self.slots):
            self.cursor.at(pos)
            size = self.cursor.read_int64()
            if size == 0:
                self.deleted.append(i)
        
        self.release_lock()
    
    def delete_tuple_by_index(self, index):
        pos = self.slots[index]
        self.cursor.at(pos)

        # op=delete marks size value to zero
        # activate function iter all tuples and check a tuple is live
        self.cursor.write_int64(0)
        self.deleted.append(index)
    
    def write_tuple_data(self, size, data_buffer):
        # insert write only page buffer
        # todo: write to wal segment for fist instead page buffer directly
        if size < HeapTuple.HEAP_TUPLE_HEADER_SIZE:
            raise Exception(f"heap tuple size underflow: {size}")

        self.cursor.at(self.slot_cursor)

        _info(f"write tuple data from={self.slot_cursor} to {self.slot_cursor+size}, size={size}")
        self.cursor.at(self.slot_cursor)
        self.cursor.write_raw(data_buffer)

    def insert(self, t):
        self.acquire_lock()

        self.tuple_count += 1
        data_buffer = t.buffer

        self.write_tuple_count()
        self.add_slot(t.size)
        self.write_tuple_data(t.size, data_buffer)

        self.release_lock()
    
    def ptype(self):
        return "heap"
 
    def update_header_buffer(self):
        header_buffer = self.ser_header()
        assert len(header_buffer) == heap_page.HEAP_PAGE_HDR_SIZE

        self.buffer[:len(header_buffer)] = header_buffer
        self.mark_dirty_flag()
    
    def set_min_key_buffer(self, min_key):
        self.cursor.at(heap_page.MIN_KEY_OFFSET)
        self.cursor.write_int64(min_key)
    
    def mark_min_key(self, min_key):
        # min_key wrote in page header is for convinience of executor 
        # it is not garuanted that min_key is actually min key in this page.
        # responsibility is up to caller.
        self.min_key = min_key
        self.set_min_key_buffer(min_key)
    
    def ser_header(self):
        cursor = buffer_cursor()

        cursor.write_int64_a(self.id)
        cursor.write_int64_a(self.type)
        cursor.write_int64_a(self.min_key)
        cursor.write_int64_a(self.tuple_count)
        cursor.write_int64_a(self.slot_cursor)

        assert len(cursor.buffer) == heap_page.HEAP_PAGE_HDR_SIZE

        return cursor.buffer
    
    @classmethod
    def parse_header_buffer(cls, buffer):
        cursor = buffer_cursor(buffer)

        return (
            cursor.read_int64(), # id
            cursor.read_int64(), # type
            cursor.read_int64(), # min_key
            cursor.read_int64(), # tuple_count
            cursor.read_int64(), # slot_cursor
        )

    def apply_header_buffer(self):
        key, type, min_key, tuple_count, slot_cursor = self.parse_header_buffer(self.buffer)
        self.cursor = buffer_cursor(self.buffer)

        self.key = key
        self.type = type
        self.min_key = min_key
        self.tuple_count = tuple_count
        self.slot_cursor = slot_cursor
