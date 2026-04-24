from core.page import page
from core.heap import heap_page

global alloc
global cache_pool

alloc = None
cache_pool = None

class page_allocator:
    def __init__(self, blkdev):
        self.blkdev = blkdev
        self.metablock = blkdev.read_metablock()
        self.cache_pool = page_cache_pool(blkdev)
        
    def palloc(self):
        new_page_id = self.metablock.inc() - 1
        print("page alloc: %d" % new_page_id)
        pg = page(new_page_id, -1, -1)
        self.cache_pool.cache(pg)
        return pg

    def hpalloc(self):
        new_page_id = self.metablock.inc() - 1
        print("heap page alloc: %d" % new_page_id)
        self.cache_pool.cache(pg)
        return heap_page(new_page_id)

class page_cache_pool:
    def __init__(self, blkdev):
        self.blkdev = blkdev
        self.pool = {}
    
    def cache(self, pg):
        assert type(pg) == page
        self.pool[pg.id] = pg

    def commit_all_pages(self):
        for id in self.pool:
            page = self.pool[id]
            self.blkdev.write_page(page)

class pg_mgr:
    def __init__(self, blk, cache_pool):
        self.blk = blk
        self.cache_pool = cache_pool
    
    def proc(self):
        for pgid in self.cache_pool.pool:
            pg = self.cache_pool.pool[pgid]
            pg.acquire_lock()
            if pg.dirty:
                blk.write_page(pg)
            pg.release_lock()

def _init_mgr_module(blkdev):
    global alloc
    alloc = page_allocator(blkdev)
    cache_pool = page_cache_pool(blkdev)
    return alloc, cache_pool
