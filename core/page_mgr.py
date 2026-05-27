from core.page import page
from utils.logging import info
from core.helper import _minkey
from core.meta import  get_metablock
from core.const import *
from core.heap import heap_page
from core.helper import _ptype, _id
from core.extent import Extent, RotateExtent
import threading

global alloc
global cache_pool
_info = lambda x: info("page_mgr", x)

alloc = None
cache_pool = None

TEMP_HEAP_PAGES_RETURNED = {}

def global_palloc(type=0):
    global alloc
    return alloc.palloc(type)

def global_hpalloc():
    global alloc
    return alloc.hpalloc()

def sys_hpalloc(sys_page_id):
    assert sys_page_id != 0
    global alloc
    return alloc.sys_hpalloc(sys_page_id)

def sys_hpalloc_ref(sys_page_id):
    assert sys_page_id != 0
    global alloc
    return alloc.sys_hpalloc_ref(sys_page_id)

def ref_page(id):
    global cache_pool
    assert id != 0
    
    try:
        _info(f"ref_page read from cache: {id}")
        pg = cache_pool.pool[id]
        return pg
    except KeyError:
        _info(f"ref_page cache miss: {id}, read from blk driver")
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
        self.metablock = get_metablock()
        self.cache_pool = page_cache_pool(blkdev)

        self.temp_ret_lock = threading.Lock()
    
    def sys_hpalloc_ref(self, page_id):
        if page_id > PAGE_MAX_SYS_ID:
            raise Exception("sys page allocated only 200 pages")

        if self.cache_pool.exists(page_id):
            return self.cache_pool.get(page_id)

        pg = heap_page(page_id)
        self.cache_pool.cache(pg)
        return pg

    def sys_hpalloc(self, page_id):
        assert page_id != 0
        if page_id > PAGE_MAX_SYS_ID:
            raise Exception("sys page allocated only 200 pages")

        if self.cache_pool.exists(page_id):
            raise Exception(f"sys heap page:{page_id} already allocated")
        
        pg = heap_page(page_id)
        self.cache_pool.cache(pg)
        return pg
    
    def alloc_sys_rotate_extent(self, page_id, page_num):
        buffer = bytearray(b"\x00" * page_num* PAGE_SIZE)
        return RotateExtent(page_id, page_num, buffer)
        
    def palloc(self, page_type=0):
        new_page_id = self.metablock.inc() - 1

        if new_page_id < PAGE_MAX_SYS_ID:
            raise Exception("palloc tried to sys pages. not allowed")

        _info("page alloc: %d" % new_page_id)
        pg = page(new_page_id, -1, -1)

        pg.type = page_type

        self.cache_pool.cache(pg)
        return pg
    
    def return_temp_heap_page(self, heap_page):
        assert _ptype(heap_page) == PAGE_TYPE_HEAP

        with self.temp_ret_lock:
            heap_page.clear()
            TEMP_HEAP_PAGES_RETURNED[_id(heap_page)] = heap_page

    def hpalloc(self, temp=False) -> heap_page:

        if temp:
            with self.temp_ret_lock:
                if len(TEMP_HEAP_PAGES_RETURNED) > 0:
                    temp_page = TEMP_HEAP_PAGES_RETURNED[TEMP_HEAP_PAGES_RETURNED.keys()[0]]
                    temp_page.clear() 
                    return temp_page

        new_page_id = self.metablock.inc() - 1

        if new_page_id < PAGE_MAX_SYS_ID:
            raise Exception(f"hpalloc tried to sys page:{new_page_id}. not allowed")

        _info("heap page alloc: %d" % new_page_id)
        pg = heap_page(new_page_id)

        self.cache_pool.cache(pg)

        return pg

    def ref_heap_page(self, id) -> heap_page:
        page = ref_page(id)
        page = page.as_heap()
        page.activate()

        return page

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
        
    def get(self, id):
        return self.pool[id]
    
    def autocommit(self):
        for id in self.pool:
            pg = self.pool[id]
            self.blkdev.write_page(pg)
    
def _init_mgr_module(blkdev):
    global alloc
    global cache_pool

    alloc = page_allocator(blkdev)
    cache_pool = alloc.cache_pool
    return alloc, cache_pool