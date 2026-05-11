from core.blk import _init_blk_driver
from core.page_mgr import _init_mgr_module
from core.wal import _init_wal_system

import threading

class DBMaster:
    def __init__(self, driver_num=0):
        self.driver_num = driver_num 
        self.blk = None
        self.alloc = None
        self.cache_pool = None
        self.procs = {}
        self.wal_writer = None
        self.wal_checkpointer = None

    def activate(self):
        blk = _init_blk_driver(self.driver_num)
        alloc, cache_pool = _init_mgr_module(blk)

        self.blk = blk
        self.alloc = alloc
        self.cache_pool = cache_pool

        self.fork_pg_wal_proc()
    
    def fork_pg_wal_proc(self):
        self.wal_writer, self.wal_checkpointer = _init_wal_system(self.blk)
        th = threading.Thread(target=self.wal_checkpointer.proc)

        self.procs["pg_checkpointer"] = th
        th.start()

        th = threading.Thread(target=self.wal_writer.proc)
        self.procs["pg_wal"] = th
        th.start()

        return th
    
    def terminate(self):
        self.wal_checkpointer.wait_to_terminate()