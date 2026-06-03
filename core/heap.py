from core.const import *
from core.page import page
from utils.buffer_cursor import buffer_cursor
from utils.dec import *
from utils.logging import info
from core.helper import _buffer
from core.wal import xlog_full_page_write

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
    
    @classmethod
    def write_xmax(cls, buffer, xmax):
        cursor = buffer_cursor(buffer)
        cursor.at(16)
        cursor.write_int64(xmax)
        return buffer
    
    @classmethod
    def get_minmax_from_buffer(cls, buffer):
        cursor = buffer_cursor(buffer)
        cursor.at(8)
        return cursor.read_int64(), cursor.read_int64()

    @classmethod
    def get_xmin_from_buffer(cls, buffer):
        cursor = buffer_cursor(buffer)
        cursor.at(8)
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
    def parse(self, buffer, schema=None):
        t = StructuredTuple(buffer)
        cursor = buffer_cursor(buffer)

        t.size = cursor.read_int64()
        t.xmin = cursor.read_int64()
        t.xmax = cursor.read_int64()
        t.reserved = cursor.read_int64()
        t.pk = cursor.read_int64()

        if schema is not None:
            t.struct(schema)

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
        self.next_page_id = None
    
    def clear(self):
        with self.lock:
            _page_id = self.id
            self.buffer = bytearray(b'\x00' * int(PAGE_SIZE))

            self.id = _page_id
            self.tuple_count = 0
            self.slots = []
            self.deleted = []
            self.type = PAGE_TYPE_HEAP

            self.update_header_buffer()
    
    def read_next_page_pointer(self):
        self.cursor.at(heap_page.HEAP_NEXT_PAGE_POINTER_OFFSET)
        v = self.cursor.read_int64()
        self.next_page_id = v
        return v
    
    def set_next_page_pointer(self, next_page_id):
        self.cursor.at(heap_page.HEAP_NEXT_PAGE_POINTER_OFFSET)
        self.next_page_id = next_page_id
        self.cursor.write_int64(next_page_id)
    
    def has_next(self):
        return self.read_next_page_pointer() != 0

    def write_tuple_count(self):
        self.pin()
        _info(f"write tuple_count to heap page:{self.id} count={self.tuple_count}")
        self.buffer[HDR_SIZE: HDR_SIZE+8] = serint64(self.tuple_count)
        self.unpin()

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

            if raw_filter_func is None or raw_filter_func(item):
                res.append(item)

        return res
    
    def raw_map(self, f, ctx=None):
        res = self._raw_map(f, ctx)
        ref_page = self
        from core.page_mgr import ref_heap_page

        while ref_page.has_next():
            ref_page = ref_heap_page(ref_page.next_page_id)
            res.extend(ref_page._raw_map(f, ctx))
        
        return list(x[1] for x in res)
    
    def _raw_map(self, f, ctx=None):
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

            if ctx is not None and not heap_page.is_visible(ctx, buffer):
                continue 

            res.append( ( HeapTuple.get_pk_from_buffer(buffer), f(buffer) ) )

        return sorted(res, key=lambda x: x[0])
    
    @classmethod
    def is_visible(cls, ctx, tuple_buffer):
        xmin, xmax = HeapTuple.get_minmax_from_buffer(tuple_buffer)
        if xmax == 0:
            return xmin <= ctx.xid
        return xmin <= ctx.xid < xmax

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

            self.pin()
            self.apply_header_buffer()
            self.load_slots_from_buffer()
            self.deleted = []

            for i, pos in enumerate(self.slots):
                self.cursor.at(pos)
                size = self.cursor.read_int64()
                if size == 0:
                    self.deleted.append(i)

            self.activated = True        
            self.read_next_page_pointer()
            self.unpin()

    def validate__raise(self):
        if len(self.slots) != self.tuple_count:
            raise Exception(f"tuple count inconsitent {self.tuple_count}, {len(self.slots)}")

        cursor = buffer_cursor(self.buffer)
        
        for s in self.slots:
            cursor.at(s)

            size = cursor.read_int64()

            if r != s:
                raise Exception(f"tuple size header not consistent {r}, {s}")

    
    def delete_tuple_by_index(self, index):
        self.pin()

        pos = self.slots[index]
        self.cursor.at(pos)

        # op=delete marks size value to zero
        # activate function iter all tuples and check a tuple is live
        self.cursor.write_int64(0)
        self.deleted.append(index)
    
        self.unpin()

    def compact(self):
        # cleanup all deletions
        pass
    
    def write_tuple_data(self, size, data_buffer):
        self.pin()
        # todo: write to wal segment for fist instead page buffer directly
        if size < HeapTuple.HEAP_TUPLE_HEADER_SIZE:
            self.unpin()
            raise Exception(f"heap tuple size underflow: {size}")

        self.cursor.at(self.slot_cursor)

        _info(f"write tuple data from={self.slot_cursor} to {self.slot_cursor+size}, size={size}")
        self.cursor.at(self.slot_cursor)
        self.cursor.write_raw(data_buffer)
        self.unpin()
     
    def capacity(self):
        return self.slot_cursor - (heap_page.SLOT_SEGMENT_OFFSET + (heap_page.SLOT_SIZE * (self.tuple_count + 1)))
    
    def usage_pct(self):
        return (PAGE_SIZE - self.capacity()) / PAGE_SIZE
    
    def possible(self, size):
        return self.capacity() >= size

    def rollback_insert(self, slot_index):
        with self.lock:
            self.pin()
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
            self.unpin()

    def before_write_data(self, wal_writer, slot_index, tuple_data):
        from core.wal import create_xlog_heap_insert_cmd
        xlog = create_xlog_heap_insert_cmd(tuple_data.xmin, self.id, slot_index, tuple_data)
        wal_writer.write_xlog(xlog)

    def raw_get(self, pk):
        page = self
        from core.page_mgr import ref_heap_page

        while True:
            with page.lock:
                page.pin()

                index = page.get_slot_index_by_pk(pk)

                if index != -1:
                    cursor = buffer_cursor(page.buffer)
                    pos = page.slots[index]

                    cursor.at(pos)
                    size = cursor.read_int64()
                    cursor.at(pos)

                    page.unpin()
                    return cursor.read(size)

                if not page.has_next():
                    page.unpin()
                    break

                acc += page.tuple_count

            page.unpin()
            page = ref_heap_page(self.next_page_id)
        
        return None

    def search(self, pk):

        index = self.search_index(pk)
        if index < 0:
            return None

        cursor = buffer_cursor(self.buffer)
        pos = self.slots[index]

        return self.read_tuple_buffer(cursor, pos)
    
    def search_index(self, pk):
        from core.page_mgr import ref_heap_page

        page = self
        acc = 0
        from core.page_mgr import ref_heap_page

        while True:
            with page.lock:
                page.pin()
                index = page.get_slot_index_by_pk(pk)

                if index != -1:
                    page.unpin()
                    return index + acc

                if not page.has_next():
                    page.unpin()
                    break

                acc += page.tuple_count
            
            page.unpin()
            page = ref_heap_page(self.next_page_id)
        
        return -1
    
    @classmethod
    def read_tuple_buffer(cls, cursor, pos):
        cursor.at(pos)
        size = cursor.read_int64() 
        cursor.at(pos)
        return cursor.read(size)
    
    def get_slot_index_by_pk(self, pk):
        cursor = buffer_cursor(_buffer(self))
        self.pin()

        for idx, pos in enumerate(self.slots):
            if idx in self.deleted:
                continue

            tuple_buffer = heap_page.read_tuple_buffer(cursor, pos)
            _pk = HeapTuple.get_pk_from_buffer(tuple_buffer)

            if pk == _pk:
                self.unpin()
                return idx
        
        self.unpin()
        return -1 

    def update(self, ctx, pk, new_tuple):
        with self.lock:
            slot_index = self.get_slot_index_by_pk(pk)
            assert slot_index != -1

            self.delete_tuple_by_index(slot_index)
            return self.insert(new_tuple, locking=False)
        
    def empty(self):
        return self.tuple_count == 0

    def copy_from(self, source):
        self.buffer[:PAGE_SIZE] = source.buffer[:PAGE_SIZE]
        self.slots = source.slots
        self.tuple_count = source.tuple_count
        self.deleted = source.deleted
        self.next_page_id = source.next_page_id
        self.activated = source.activated
        self.update_header_buffer()

    def split_insert(self, t, ctx):
        self.lock.acquire()
        assert self.tuple_count > 1
        
        new_heap_page  = ctx.allocator.hpalloc()
        new_heap_page.lock.acquire()

        temp_heap_page = ctx.allocator.hpalloc(temp=True)
        # TODO return temp heap page

        tuples = []
        cursor = buffer_cursor(self.buffer)

        for pos in self.slots:
            buffer = heap_page.read_tuple_buffer(cursor, pos)
            tuples.append( ( StructuredTuple.get_pk_from_buffer(buffer), buffer) )
        
        tuples_sorted = sorted(tuples, key=lambda x: x[0])
        mid_index = int(len(tuples_sorted) / 2)

        grp0 = tuples_sorted[:mid_index]
        grp0_min_key = grp0[0][0]

        for idx, t0 in enumerate(grp0):
            _, buffer = t0
            size = len(buffer)

            temp_heap_page.tuple_count = idx + 1
            temp_heap_page.add_slot(size)
            temp_heap_page.write_tuple_data(size, buffer) 

        temp_heap_page.write_tuple_count()
        temp_heap_page.mark_min_key(grp0_min_key)
        temp_heap_page.update_header_buffer()

        assert len(temp_heap_page.buffer) == PAGE_SIZE

        self.copy_from(temp_heap_page)

        grp1 = tuples_sorted[mid_index:]
        grp1_min_key = grp1[0][0]

        for idx, t1 in enumerate(grp1):
            _, buffer = t1
            size = len(buffer)

            new_heap_page.tuple_count = idx + 1
            new_heap_page.add_slot(size)
            new_heap_page.write_tuple_data(size, buffer) 

        new_heap_page.write_tuple_count()
        new_heap_page.mark_min_key(grp1_min_key)
        new_heap_page.update_header_buffer()

        target = None
        if t.pk < grp1_min_key:
            target = self
        else:
            target = new_heap_page

        target.tuple_count += 1
        target.add_slot(t.size)
        target.write_tuple_data(t.size, t.buffer)
        target.update_header_buffer()

        self.lock.release()
        new_heap_page.lock.release()

        ctx.allocator.return_temp_heap_page(temp_heap_page)

        return new_heap_page
        
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

        if self.empty() and ctx is not None:
            xlog_full_page_write(ctx.wal_writer, ctx.xid, self)

        assert len(self.slots) == self.tuple_count
        locking and self.lock.release()

        return slot_index
    
    def ptype(self):
        return "heap"
 
    def update_header_buffer(self):
        self.pin()
        header_buffer = self.ser_header()
        assert len(header_buffer) == heap_page.HEAP_PAGE_HDR_SIZE

        self.buffer[:len(header_buffer)] = header_buffer
        self.mark_dirty_flag()
        self.unpin()
    
    def set_min_key_buffer(self, min_key):
        self.pin()
        self.cursor.at(heap_page.MIN_KEY_OFFSET)
        self.cursor.write_int64(min_key)
        self.unpin()
    
    def mark_min_key(self, min_key):
        # min_key wrote in page header is for convinience of executor 
        # it is not garuanted that min_key is actually min key in this page.
        # responsibility is up to caller.
        self.pin()
        self.min_key = min_key
        self.set_min_key_buffer(min_key)
        self.unpin()
    
    def ser_header(self):
        self.pin()
        cursor = buffer_cursor()

        cursor.write_int64_a(self.id)
        cursor.write_int64_a(self.type)
        cursor.write_int64_a(self.min_key)
        cursor.write_int64_a(self.tuple_count)
        cursor.write_int64_a(self.slot_cursor)

        assert len(cursor.buffer) == heap_page.HEAP_PAGE_HDR_SIZE
        self.unpin()
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

def insert_with_grow(alloc_func, heap_page_to_insert, t, ctx=None):
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
        heap_page_to_insert.insert(t, ctx=ctx)
        return heap_page_to_insert


