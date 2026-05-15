import os
from utils.dec import *
from utils.logging import info
from utils.buffer_cursor import buffer_cursor
from core.const import PAGE_TYPE_META, PAGE_HDR_SIZE, SYSTEM_PAGE_ALLOC, META_SYS_PAGE_ID
import threading

_info = lambda x: info("meta", x)

global META
META = None

global META_PAGE
META_PAGE = None

class metablock:
    def __init__(self, blkdev):
        assert META is None
        self.blkdev = blkdev
        self.next_page = None
        self.checkpointer_lsn_begin = None
        self.checkpointer_lsn_end = None
        self.checkpointer_lsn_committed = None
        self.meta_page = None
        self.current_wal_seg_file = None

        self.lock = threading.Lock()
        self.write_lock = threading.Lock()
    
    def __repr__(self):
        return f"next_page={self.next_page},lsn_committed={self.checkpointer_lsn_committed},lsn_begin={self.checkpointer_lsn_begin},lsn_end={self.checkpointer_lsn_end},wal_seg_file={self.current_wal_seg_file},wal_seg_pos={self.last_committed_wal_seg_pos}"

    def read_meta_page(self):
        global META_PAGE
        if META_PAGE is None:
            META_PAGE = self.blkdev.read_page(META_SYS_PAGE_ID)
        return META_PAGE

    def init(self):
        _info("metablock initializing")
        self.meta_page = self.read_meta_page()
        cursor = buffer_cursor(self.meta_page.buffer)

        cursor.at(PAGE_HDR_SIZE)

        self.next_page = cursor.read_int64()

        self.checkpointer_lsn_begin = cursor.read_int64()
        self.checkpointer_lsn_end = cursor.read_int64()
        self.checkpointer_lsn_committed = cursor.read_int64()

        self.current_wal_seg_file = cursor.read_char(16)
        self.last_committed_wal_seg_file = cursor.read_char(16)
        self.last_committed_wal_seg_pos = cursor.read_int64()

        _info(f"metablock loaded: {self}")
    
    def set_next_page(self, next_page):
        self.next_page = next_page

        offset = PAGE_HDR_SIZE
        cursor = buffer_cursor(self.meta_page.buffer)
        cursor.at(offset)
        cursor.write_int64(next_page)

    def set_next_page_with_commit(self, next_page):
        with self.lock:
            self.set_next_page(next_page)
            self.commit_metablock()

    def set_begin_lsn_with_commit(self, lsn):
        with self.lock:
            assert self.checkpointer_lsn_begin < lsn

            offset = PAGE_HDR_SIZE + 8
            cursor = buffer_cursor(self.meta_page.buffer)
            cursor.at(offset)
            cursor.write_int64(lsn)

            self.checkpointer_lsn_begin = lsn
            self.commit_metablock()

    def set_end_lsn_with_commit(self, lsn):
        with self.lock:
            assert self.checkpointer_lsn_end < lsn
            offset = PAGE_HDR_SIZE + 16
            cursor = buffer_cursor(self.meta_page.buffer)
            cursor.at(offset)
            cursor.write_int64(lsn)

            self.checkpointer_lsn_end = lsn
            self.commit_metablock()

    def set_commit_lsn_with_commit(self, lsn):
        with self.lock:
            assert self.checkpointer_lsn_committed < lsn

            offset = PAGE_HDR_SIZE + 24
            cursor = buffer_cursor(self.meta_page.buffer)
            cursor.at(offset)
            cursor.write_int64(lsn)

            self.checkpointer_lsn_committed = lsn
            self.commit_metablock()

    def set_current_wal_seg_file_with_commit(self, file):
        with self.lock:
            self.current_wal_seg_file = file
            assert len(file) == 16

            offset = PAGE_HDR_SIZE + 32
            cursor = buffer_cursor(self.meta_page.buffer)
            cursor.at(offset)
            cursor.write_char(file)
            self.commit_metablock()
        
    def set_last_committed_wal_seg_file(self, file):
        with self.lock:
            self.last_committed_wal_seg_file = file
            assert len(file) == 16

            offset = PAGE_HDR_SIZE + 48
            cursor = buffer_cursor(self.meta_page.buffer)
            cursor.at(offset)
            cursor.write_char(file)
            self.commit_metablock()

    def set_last_committed_wal_seg_pos_with_commit(self, pos):
        with self.lock:
            self.last_committed_wal_seg_pos = pos

            offset = PAGE_HDR_SIZE + 64
            cursor = buffer_cursor(self.meta_page.buffer)
            cursor.at(offset)
            cursor.write_int64(pos)
            self.commit_metablock()
    
    def get_value(self, key):
        with self.lock:
            return getattr(self, key)
        
    def delete_old_wal_files(self):
        try:
            os.remove(START_WAL_SEGFILE)
        except FileNotFoundError:
            pass
    
    def bootstrap(self):
        self.delete_old_wal_files()

        self.meta_page = self.read_meta_page()
        self.meta_page.type = PAGE_TYPE_META
        self.meta_page.id = META_SYS_PAGE_ID

        self.meta_page.update_header_buffer()

        cursor = buffer_cursor(self.meta_page.buffer)
        cursor.at(PAGE_HDR_SIZE)

        cursor.write_int64(SYSTEM_PAGE_ALLOC)
        cursor.write_int64(0)
        cursor.write_int64(0)
        cursor.write_int64(0)
        cursor.write_char(START_WAL_SEGFILE, 16)
        cursor.write_char(START_WAL_SEGFILE, 16)
        cursor.write_int64(0)

        _info(f"bootstrap meta page: next_page={SYSTEM_PAGE_ALLOC} lsn={0},{0},{0} start_wal_seg_file={START_WAL_SEGFILE}")

        self.next_page = SYSTEM_PAGE_ALLOC
        self.checkpointer_lsn_begin = 0
        self.checkpointer_lsn_end = 0
        self.checkpointer_lsn_committed = 0
        self.current_wal_seg_file = START_WAL_SEGFILE
        self.last_committed_wal_seg_file = START_WAL_SEGFILE
        self.last_committed_wal_seg_pos = 0 # TODO WAL SEG HEADER OFFSET

        self.blkdev.write_page(self.meta_page)
    
    def inc(self):
        self.set_next_page(self.next_page + 1)
        return self.next_page

    def commit_metablock(self):
        _info(f"commit metablodk: {self}")
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