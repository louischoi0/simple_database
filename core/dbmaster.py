from core.blk import _init_blk_driver
from core.page_mgr import _init_mgr_module

class DBMaster:
    def __init__(self, driver_num=0):
        self.driver_num = driver_num 
        self.blk = None
        self.alloc = None
        self.cache_pool = None

    def activate(self):
        blk = _init_blk_driver(self.driver_num)
        alloc, cache_pool = _init_mgr_module(blk)

        self.blk = blk
        self.alloc = alloc
        self.cache_pool = cache_pool