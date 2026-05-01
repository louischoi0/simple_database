from core.page import page
from core.heap import heap_page
from utils.logging import info
from core.helper import _minkey

global alloc
global cache_pool
_info = lambda x: info("page_mgr", x)

alloc = None
cache_pool = None

def global_palloc():
    global alloc
    return alloc.palloc()

def sys_hpalloc(sys_page_id):
    global alloc
    return alloc.sys_hpalloc(sys_page_id)

def ref_sys_page(id):
    global cache_pool
    
    try:
        return cache_pool.sys_pool[id]
    except KeyError:
        page = cache_pool.blkdev.read_page(id)
        cache_pool.sys_cache(page)
        return page

def ref_page(id):
    global cache_pool
    
    try:
        return cache_pool.pool[id]
    except KeyError:
        page = cache_pool.blkdev.read_page(id)
        cache_pool.cache(page)
        return page

def ref_minkey(id):
    pg = ref_page(id)
    return _minkey(pg)

class page_allocator:
    def __init__(self, blkdev):
        self.blkdev = blkdev
        self.metablock = blkdev.read_metablock()
        self.cache_pool = page_cache_pool(blkdev)

    def sys_hpalloc(self, page_id):
        if self.cache_pool.sys_exists(page_id):
            raise Exception(f"sys heap page:{page_id} already allocated")
        
        pg = heap_page(page_id)
        self.cache_pool.sys_cache(pg)
        return pg
        
    def palloc(self):
        new_page_id = self.metablock.inc() - 1
        _info("page alloc: %d" % new_page_id)
        pg = page(new_page_id, -1, -1)
        self.cache_pool.cache(pg)
        return pg

    def hpalloc(self):
        new_page_id = self.metablock.inc() - 1
        _info("heap page alloc: %d" % new_page_id)
        pg = heap_page(new_page_id)
        self.cache_pool.cache(pg)
        return pg

class page_cache_pool:
    def __init__(self, blkdev):
        self.blkdev = blkdev
        self.pool = {}
        self.sys_pool = {} # sys pool page is never evicted
    
    def exists(self, id):
        return id in self.pool
    
    def sys_exists(self, id):
        return id in self.sys_pool
    
    def cache(self, pg):
        if pg is None:
            raise Exception("try to cache Null page")
        _info(f"cache page {pg.id}")
        self.pool[pg.id] = pg
    
    def sys_cache(self, pg):
        if pg is None:
            raise Exception("try to cache Null sys page")
        _info(f"cache page {pg.id}")
        self.sys_pool[pg.id] = pg

    def commit_all_pages(self):
        for id in self.pool:
            page = self.pool[id]
            self.blkdev.write_page(page)

class pg_mgr:
    def __init__(self, blk, cache_pool):
        self.blk = blk
        self.cache_pool = cache_pool
    
    def proc(self):
        while True:
            self.commit_dirty_pages()

            from time import sleep
            sleep(1)

    def commit_dirty_pages(self):
        for pgid in self.cache_pool.pool:
            pg = self.cache_pool.pool[pgid]
            pg.acquire_lock()
            if pg.dirty:
                blk.write_page(pg)
            pg.release_lock()

def _init_mgr_module(blkdev):
    global alloc
    global cache_pool

    alloc = page_allocator(blkdev)
    cache_pool = alloc.cache_pool

    return alloc, cache_pool
