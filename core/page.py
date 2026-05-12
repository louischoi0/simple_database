from core.const import *  
from core.helper import _ptype
from utils.buffer_cursor import buffer_cursor
from threading import Lock

def serint64(value: int) -> bytes:
    return value.to_bytes(8, byteorder=BYTE_ORDER, signed=True)

def toint64(buf: bytes) -> int:
    return int.from_bytes(buf, byteorder=BYTE_ORDER, signed=True)

def is_heap_page(page):
    if hasattr(page, "page"):
        return page.page.type == PAGE_TYPE_HEAP
    else:
        return page.type == PAGE_TYPE_HEAP
    
def is_btree_page(page):
    if hasattr(page, "page"):
        return page.page.type in (PAGE_TYPE_DATA, PAGE_TYPE_INTERNAL, PAGE_TYPE_ROOT, )
    else:
        return page.type in (PAGE_TYPE_DATA, PAGE_TYPE_INTERNAL, PAGE_TYPE_ROOT, )

def is_meta_page(page):
    return page.type == PAGE_TYPE_META

def cast_page(page):
    if is_btree_page(page):
        from core.btree import bt_node
        return bt_node.as_btnode(page)
    elif is_heap_page(page):
        return page.as_heap()
    elif is_meta_page(page):
        return page
    else:
        raise Exception(f"unknown page type: {_ptype(page)}")

def get_page_name(type):
    if type == PAGE_TYPE_DATA:
        return "btree data"
    elif type == PAGE_TYPE_INTERNAL:
        return "btree internal"
    elif type == PAGE_TYPE_ROOT:
        return "btree root"
    elif type == PAGE_TYPE_HEAP:
        return "heap"
    else:
        return "invalid page"

class page:
    def __init__(self, page_id, type, min_key):
        self.id = page_id
        self.min_key = min_key
        self.type = type
        self.buffer = bytearray(b'\x00' * int(PAGE_SIZE))
        self.dirty = False

        self.lock = Lock()
        self.pin = 0

    def checksum(self):
        import hashlib
        enc = hashlib.md5()
        enc.update(self.buffer)
        return enc.hexdigest()
    
    def cursor(self):
        return buffer_cursor(self.buffer)
    
    def pin(self):
        self.pin += 1
    
    def unpin(self):
        self.pin -= 1

    def acquire_lock(self):
        self.lock.acquire()

    def release_lock(self):
        self.lock.release()
    
    def ptype(self):
        return "page"
    
    def update_header_buffer(self):
        header_buffer = self.ser_header()
        assert len(header_buffer) == HDR_SIZE
        self.buffer[:len(header_buffer)] = header_buffer
        self.mark_dirty_flag()
    
    def mark_dirty_flag(self):
        self.dirty = True 
    
    def clear_dirty_flag(self):
        self.dirty = False
    
    def ser_header(self):
        return (
            serint64(self.id) +
            serint64(self.type) +
            serint64(self.min_key)
        )

    @classmethod
    def parse_header_buffer(cls, buffer):
        return (
            toint64(buffer[0:8]),
            toint64(buffer[8:16]),
            toint64(buffer[16:24]),
        )
    
    def apply_header_buffer(self):
        id, type, min_key = self.parse_header_buffer(self.buffer)
        self.id = id
        self.type = type
        self.min_key
    
    def as_heap(self):
        assert self.type == PAGE_TYPE_HEAP

        from core.heap import heap_page

        p = heap_page(self.id)

        p.buffer = self.buffer
        p.lock = self.lock

        with p.lock:
            p.apply_header_buffer()

        return p
