from utils.dec import *
from core.page import cast_page
from core.helper import _id, _buffer
from utils.logging import info
from utils.buffer_cursor import buffer_cursor
from core.const import PAGE_TYPE_META, PAGE_HDR_SIZE, SYSTEM_PAGE_ALLOC, META_SYS_PAGE_ID

_info = lambda x: info("meta", x)

global META
META = None

class metablock:
    def __init__(self, blkdev):
        assert META is None
        self.blkdev = blkdev
        self.next_page = None
        self.checkpointer_lsn_begin = None
        self.checkpointer_lsn_end = None
        self.checkpointer_lsn_committed = None
        self.meta_page = None
    
    def __repr__(self):
        return f"next_page={self.next_page},lsn_committed={self.checkpointer_lsn_committed},lsn_begin={self.checkpointer_lsn_begin},lsn_end={self.checkpointer_lsn_end}"

    def init(self):
        self.meta_page = self.blkdev.read_page(META_SYS_PAGE_ID)
        cursor = buffer_cursor(self.meta_page.buffer)

        cursor.at(PAGE_HDR_SIZE)
        self.next_page = cursor.read_int64()
        self.checkpointer_lsn_begin = cursor.read_int64()
        self.checkpointer_lsn_end = cursor.read_int64()
        self.checkpointer_lsn_committed = cursor.read_int64()
        _info(f"metablock loaded: {self}")
    
    def set_commit_lsn(self, lsn):
        assert self.checkpointer_lsn_committed < lsn
        self.checkpointer_lsn_committed = lsn

    def set_begin_lsn_with_commit(self, lsn):
        assert self.checkpointer_lsn_begin < lsn
        self.checkpointer_lsn_begin = lsn
        self.commit_metablock()

    def set_end_lsn_with_commit(self, lsn):
        assert self.checkpointer_lsn_end < lsn
        self.checkpointer_lsn_end = lsn
        self.commit_metablock()

    def set_commit_lsn_with_commit(self, lsn):
        assert self.checkpointer_lsn_committed < lsn
        self.checkpointer_lsn_committed = lsn
        self.commit_metablock()

    def begin_lsn(self, lsn):
        assert self.checkpointer_lsn_begin < lsn
        self.checkpointer_lsn_committed = lsn
    
    def bootstrap(self):
        self.meta_page = self.blkdev.read_page(META_SYS_PAGE_ID)
        self.meta_page.type = PAGE_TYPE_META
        self.meta_page.id = META_SYS_PAGE_ID

        self.meta_page.update_header_buffer()

        cursor = buffer_cursor(self.meta_page.buffer)
        cursor.at(PAGE_HDR_SIZE)

        cursor.write_int64(SYSTEM_PAGE_ALLOC)
        cursor.write_int64(0)
        cursor.write_int64(0)
        cursor.write_int64(0)

        _info(f"bootstrap meta page: next_page={SYSTEM_PAGE_ALLOC} lsn={0},{0},{0}")

        self.next_page = SYSTEM_PAGE_ALLOC
        self.checkpointer_lsn_begin = 0
        self.checkpointer_lsn_end = 0
        self.checkpointer_lsn_committed = 0

        self.blkdev.write_page(self.meta_page)
    
    def set_next_page(self, next_page):
        self.next_page = next_page
    
    def inc(self):
        self.next_page += 1
        return self.next_page

    def commit_metablock(self):
        self.blkdev.write_page(self.meta_page)

def get_metablock():
    return META
    
def get_meta_attr(key):
    global META
    return getattr(META, key)

def _init_meta_system(blkdev):
    global META
    META = metablock(blkdev)
    META.init()
    return META