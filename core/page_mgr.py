from core.page import page
from utils.logging import info
from core.helper import _minkey
from core.const import *
from core.heap import heap_page
import threading

global alloc
global cache_pool
_info = lambda x: info("page_mgr", x)

alloc = None
cache_pool = None

def global_palloc():
    global alloc
    return alloc.palloc()

def global_hpalloc():
    global alloc
    return alloc.hpalloc()

def sys_hpalloc(sys_page_id):
    global alloc
    return alloc.sys_hpalloc(sys_page_id)

def sys_hpalloc_ref(sys_page_id):
    global alloc
    return alloc.sys_hpalloc_ref(sys_page_id)

def ref_page(id):
    global cache_pool
    
    try:
        return cache_pool.pool[id]
    except KeyError:
        page = cache_pool.blkdev.read_page(id)
        cache_pool.cache(page)
        return page

def ref_heap_page(id):
    page = ref_page(id)
    page = page.as_heap()
    page.activate()
    return page

def ref_btree_page(id):
    from core.btree import bt_node
    page = ref_page(id)
    vnode = bt_node.as_btnode(page)
    vnode.activate()
    vnode.validate()
    return vnode

def global_write_page(pg):
    global alloc
    return alloc.blkdev.write_page(pg)

def ref_minkey(id):
    pg = ref_page(id)
    return _minkey(pg)

class page_allocator:
    def __init__(self, blkdev):
        self.blkdev = blkdev
        self.metablock = blkdev.read_metablock()
        self.cache_pool = page_cache_pool(blkdev)
    
    def sys_hpalloc_ref(self, page_id):
        if page_id > PAGE_MAX_SYS_ID:
            raise Exception("sys page allocated only 200 pages")

        if self.cache_pool.exists(page_id):
            return self.cache_pool.get(page_id)

        pg = heap_page(page_id)
        self.cache_pool.cache(pg)
        return pg

    def sys_hpalloc(self, page_id):
        if page_id > PAGE_MAX_SYS_ID:
            raise Exception("sys page allocated only 200 pages")

        if self.cache_pool.exists(page_id):
            raise Exception(f"sys heap page:{page_id} already allocated")
        
        pg = heap_page(page_id)
        self.cache_pool.cache(pg)
        return pg
        
    def palloc(self):
        new_page_id = self.metablock.inc() - 1

        if new_page_id < PAGE_MAX_SYS_ID:
            raise Exception("palloc tried to sys pages. not allowed")

        _info("page alloc: %d" % new_page_id)
        pg = page(new_page_id, -1, -1)
        self.cache_pool.cache(pg)
        return pg

    def hpalloc(self):

        new_page_id = self.metablock.inc() - 1
        if new_page_id < PAGE_MAX_SYS_ID:
            raise Exception(f"hpalloc tried to sys page numbered {new_page_id}. not allowed")

        _info("heap page alloc: %d" % new_page_id)
        pg = heap_page(new_page_id)
        self.cache_pool.cache(pg)
        return pg

class page_cache_pool:
    def __init__(self, blkdev):
        self.blkdev = blkdev
        self.pool = {}
    
    def exists(self, id):
        return id in self.pool
    
    def cache(self, pg):
        if pg is None:
            raise Exception("try to cache Null page")
        _info(f"cache page {pg.id}")
        self.pool[pg.id] = pg

    def commit_all_pages(self):
        for id in self.pool:
            page = self.pool[id]
            self.blkdev.write_page(page)
        
    def get(self, id):
        return self.pool[id]

class PageManager:
    def __init__(self, blk, cache_pool):
        self.blk = blk
        self.cache_pool = cache_pool
        self.lock = threading.Lock()
        self.exit_signal = False
    
    def proc(self):
        _info("start page manager process")
        while True:
            if self.exit_signal:
                _info("exit signal received, terminate page manager process.")
                break

            self.lock.acquire()
            self.commit_dirty_pages()
            self.lock.release()

            from time import sleep
            sleep(0.5)
    
    def wait_to_terminate(self):
        self.lock.acquire()
        self.commit_dirty_pages()
        self.exit_signal = True

    def commit_dirty_pages(self):
        for pgid in self.cache_pool.pool:
            pg = self.cache_pool.pool[pgid]
            pg.acquire_lock()
            if pg.dirty:
                self.blk.write_page(pg)
                pg.clear_dirty_flag()
            pg.release_lock()

def _init_mgr_module(blkdev):
    global alloc
    global cache_pool

    alloc = page_allocator(blkdev)
    cache_pool = alloc.cache_pool

    return alloc, cache_pool
