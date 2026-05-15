from core.const import *
from core.page import page
from utils.buffer_cursor import buffer_cursor
from utils.dec import *
from utils.logging import info
from core.helper import _buffer

XFLAG_SIZE = 8
_info = lambda msg: info("heap", msg)


class HeapTuple:
    HEAP_TUPLE_HEADER_SIZE = 32 # size, xmin, xmax, reserved (null bit mask for structured heap tuple)
    HEAP_TUPLE_RESERVED_SIZE = 8

    def __init__(self, buffer: bytearray):
        self.buffer: bytearray = buffer
        cursor = buffer_cursor(buffer)
        self.size = cursor.read_int64()
        self.xmin, self.xmax = cursor.read_int64(), cursor.read_int64()
    
    def ser(self):
        return self.buffer
    
    @classmethod
    def parse(self, buffer):
        return toint64(buffer)
    
    @classmethod
    def get_pk_from_buffer(cls, buffer):
        cursor = buffer_cursor(buffer)
        cursor.at(HeapTuple.HEAP_TUPLE_HEADER_SIZE)
        return cursor.read_int64()

class StructuredTuple(HeapTuple):
    def __init__(self, buffer):
        super(StructuredTuple, self).__init__(buffer)
        self.structured_data = {}
        self.data = []
        self.pk = None

    def get(self, key):
        return self.structured_data[key]

    def struct(self, schema):
        cursor = buffer_cursor(self.buffer)
        cursor.advance(HeapTuple.HEAP_TUPLE_HEADER_SIZE)

        for idx, c in enumerate(schema.col_arr):
            assert c.pos == idx
            value = cursor.read_dynamic_type_a(c.type.value)
            self.structured_data[c.name] = value
            self.data.append(value)

        self.pk = self.data[0]
        return self.structured_data
    
    def get_null_flag_buffer(self):
        cursor = buffer_cursor(self.buffer)
        cursor.at(HeapTuple.HEAP_TUPLE_HEADER_SIZE - 8)

        return cursor.read_int64()
    
    def get_null_flag(self, colnum):
        value = self.get_null_flag_buffer()
        return bool(value & (1 << colnum))
    
    def set_null_flag(self, colnum, flag):
        cursor = buffer_cursor(self.buffer)

        value = self.get_null_flag_buffer()
        value = (value & ~(1 << colnum)) | (int(flag) << colnum)

        cursor.at(HeapTuple.HEAP_TUPLE_HEADER_SIZE - 8)
        cursor.write_int64_a()
    
    def set_xmin(self, xmin):
        self.xmin = xmin
        cursor = buffer_cursor(self.buffer)
        cursor.at(8)
        cursor.write_int64(xmin)

    def set_xmax(self, xmax):
        self.xmax = xmax
        cursor = buffer_cursor(self.buffer)
        cursor.at(16)
        cursor.write_int64(xmax)

    @classmethod
    def parse(self, buffer):
        t = StructuredTuple(buffer)
        cursor = buffer_cursor(buffer)

        t.size = cursor.read_int64()
        t.xmin = cursor.read_int64()
        t.xmax = cursor.read_int64()
        t.reserved = cursor.read_int64()
        t.pk = cursor.read_int64()

        return t
    
    @classmethod
    def load(self, schema, dictionary, version=None): 
        cursor = buffer_cursor()
        cursor.pad_a(HeapTuple.HEAP_TUPLE_HEADER_SIZE)

        from core.catalog import get_type_val

        for idx, c in enumerate(schema.col_arr):
            if idx == 0 and c.type_val != get_type_val("int"):
                raise Exception("first column must be int64 as primary key.")

            assert c.pos == idx
            value = dictionary[c.name]
            cursor.write_dynamic_type_a(c.type.value, value)

        size = len(cursor.buffer)
        cursor.at(0) 
        cursor.write_int64(size)

        if version is None:
            cursor.pad(XFLAG_SIZE * 2)
        else:
            cursor.write_int64(version[0])
            cursor.write_int64(version[1])
        
        return StructuredTuple.parse(cursor.buffer)

class heap_page(page):
    SLOT_SIZE = 4
    HEAP_PAGE_HDR_SIZE = 40
    SLOT_SEGMENT_OFFSET = HEAP_PAGE_HDR_SIZE
    MIN_KEY_OFFSET = 16
    TAIL_SEGMENT_SIZE = 32
    TUPLE_SEGMENT_OFFSET = PAGE_SIZE - TAIL_SEGMENT_SIZE
    HEAP_NEXT_PAGE_POINTER_OFFSET = PAGE_SIZE - 8
    HEAP_PAGE_HDR_SIZE = HDR_SIZE + 16

    def __init__(self, page_id):
        super().__init__(page_id, PAGE_TYPE_HEAP, -1)
        self.tuple_count = 0
        self.slot_cursor = PAGE_SIZE - heap_page.TAIL_SEGMENT_SIZE
        self.slots = []
        self.deleted = []
        self.cursor = buffer_cursor(self.buffer)
        self.activated = False
    
    def read_next_page_pointer(self):
        self.cursor.at(heap_page.HEAP_NEXT_PAGE_POINTER_OFFSET)
        return self.cursor.read_int64()
    
    def set_next_page_pointer(self, next_page_id):
        self.cursor.at(heap_page.HEAP_NEXT_PAGE_POINTER_OFFSET)
        self.cursor.write_int64(next_page_id)
    
    def has_next(self):
        return self.read_next_page_pointer() != 0

    def write_tuple_count(self):
        _info(f"write tuple_count to heap page:{self.id} count={self.tuple_count}")
        self.buffer[HDR_SIZE: HDR_SIZE+8] = serint64(self.tuple_count)

    def add_slot(self, tuple_size):
        last = self.slot_cursor
        self.slot_cursor -= tuple_size

        if tuple_size < HeapTuple.HEAP_TUPLE_HEADER_SIZE:
            raise Exception(f"heap tuple size smaller than HEAP_TUPLE_HEADER_SIZE, {tuple_size}")

        if self.slot_cursor < heap_page.SLOT_SEGMENT_OFFSET:
            raise Exception(f"heap page overflow error: tried to write tuple data at pos:{self.slot_cursor}, cap:{self.capacity()}")

        slot_buffer_offset = heap_page.SLOT_SEGMENT_OFFSET + ((self.tuple_count - 1) * self.SLOT_SIZE)
        _info(f"add slot to heap page:{self.id} index={self.tuple_count-1}, offset={slot_buffer_offset}, value={self.slot_cursor}, tuple_size={last - self.slot_cursor}, cap:{self.capacity()}")

        self.cursor.at(slot_buffer_offset)
        self.cursor.write_int32(self.slot_cursor)

        self.slots.append(self.slot_cursor)
        return len(self.slots) - 1
    
    def raw_get(self, pk):
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
            ncursor = buffer_cursor(buffer)
            ncursor.at(HeapTuple.HEAP_TUPLE_HEADER_SIZE)
            _pk = ncursor.read_int64()

            if _pk == pk:
                return buffer

        return None
    
    def raw_filter(self, f, raw_filter_func):
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
            item = f(buffer)

            if raw_filter_func(item):
                res.append(item)

        return res
    
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
        with self.lock:
            if self.activated:
                return

            self.apply_header_buffer()
            self.load_slots_from_buffer()
            self.deleted = []

            for i, pos in enumerate(self.slots):
                self.cursor.at(pos)
                size = self.cursor.read_int64()
                if size == 0:
                    self.deleted.append(i)

            self.activated = True        
    
    def delete_tuple_by_index(self, index):
        pos = self.slots[index]
        self.cursor.at(pos)

        # op=delete marks size value to zero
        # activate function iter all tuples and check a tuple is live
        self.cursor.write_int64(0)
        self.deleted.append(index)
    
    def compact(self):
        # cleanup all deletions
        pass
    
    def write_tuple_data(self, size, data_buffer):
        # todo: write to wal segment for fist instead page buffer directly
        if size < HeapTuple.HEAP_TUPLE_HEADER_SIZE:
            raise Exception(f"heap tuple size underflow: {size}")

        self.cursor.at(self.slot_cursor)

        _info(f"write tuple data from={self.slot_cursor} to {self.slot_cursor+size}, size={size}")
        self.cursor.at(self.slot_cursor)
        self.cursor.write_raw(data_buffer)
     
    def capacity(self):
        return self.slot_cursor - (heap_page.SLOT_SEGMENT_OFFSET + (heap_page.SLOT_SIZE * (self.tuple_count + 1)))
    
    def possible(self, size):
        return self.capacity() >= size

    def rollback_insert(self, slot_index):

        with self.lock:
            cursor = buffer_cursor(self.buffer)
            pos = self.slots[slot_index]

            self.tuple_count -= 1
            self.slots = self.slots[:slot_index] + self.slots[slot_index+1:]

            cursor.at(pos)
            tuple_size = cursor.read_int64()
            cursor.at(pos)
            cursor.pad(tuple_size)

            self.write_tuple_count()
            self.update_header_buffer()

    def before_write_data(self, wal_writer, slot_index, tuple_data):
        from core.wal import create_xlog_heap_insert_cmd
        xlog = create_xlog_heap_insert_cmd(tuple_data.xmin, self.id, slot_index, tuple_data)
        wal_writer.write_xlog(xlog)
    
    def get_slot_index_by_pk(self, pk):
        cursor = buffer_cursor(_buffer(self))

        for idx, pos in enumerate(self.slots):
            if idx in self.deleted:
                continue

            cursor.at(pos)

            size = cursor.read_int64() 
            cursor.at(pos)
            tuple_buffer = cursor.read(size)
            _pk = HeapTuple.get_pk_from_buffer(tuple_buffer)

            if pk == _pk:
                return idx
        return -1 

    def update(self, pk, new_tuple):
        with self.lock:
            slot_index = self.get_slot_index_by_pk(pk)
            assert slot_index != -1

            self.delete_tuple_by_index(slot_index)
            return self.insert(new_tuple, locking=False)

    def insert(self, t, ctx=None, locking=True):
        locking and self.lock.acquire()

        if ctx is not None:
            t.set_xmin(ctx.xid)

        assert self.id != NULL_PAGE

        self.tuple_count += 1
        data_buffer = t.buffer

        if t.size > self.capacity():
            return -1

        slot_index = len(self.slots)

        if ctx is not None:
            self.before_write_data(ctx.wal_writer, slot_index, t)

        self.write_tuple_count()
        self.add_slot(t.size)
        self.write_tuple_data(t.size, data_buffer)

        self.update_header_buffer()
        locking and self.lock.release()
        return slot_index
    
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

def grow(alloc_func, overflow_page, t):
    # caller must be holding page lock
    _info(f"grow page {overflow_page.id} to insert tuple sized: {t.size}, cap={overflow_page.capacity()}")

    heap_page = alloc_func()

    overflow_page.set_next_page_pointer(heap_page.id)
    overflow_page.mark_dirty_flag()
    return heap_page

def insert_with_grow(alloc_func, heap_page_to_insert, t):
    heap_page_to_insert.acquire_lock()

    next_page_id = heap_page_to_insert.read_next_page_pointer()
    from core.page_mgr import ref_page

    while next_page_id != NULL_PAGE:
        heap_page_to_insert.release_lock()
        heap_page_to_insert = ref_page(next_page_id)
        heap_page_to_insert.activate()
        heap_page_to_insert.acquire_lock()

        next_page_id = heap_page_to_insert.read_next_page_pointer()
    
    if t.size > heap_page_to_insert.capacity():
        new_page = grow(alloc_func, heap_page_to_insert, t)
        heap_page_to_insert.release_lock()
        return new_page

    else:
        heap_page_to_insert.release_lock()
        heap_page_to_insert.insert(t)
        return heap_page_to_insert



