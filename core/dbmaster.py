import os
import asyncio

from core.blk import _init_blk_driver
from core.page_mgr import _init_mgr_module
from core.wal import _init_wal_system
from core.lock import _init_lock_system
from core.meta import _init_meta_system
from core.server import DBServer
from core.tx import _init_transaction_system

import threading

class DBMaster:
    def __init__(self, driver_num=0):
        self.driver_num = driver_num 
        self.blk = None
        self.meta = None
        self.alloc = None
        self.cache_pool = None
        self.procs = {}
        self.wal_writer = None
        self.wal_checkpointer = None
        self.tx_mgr = None

        self.server = None
        self.background_proc_disabled = False

    def activate(self):
        blk = _init_blk_driver(self.driver_num)
        self.meta = _init_meta_system(blk)

        self.alloc, self.cache_pool = _init_mgr_module(blk)
        _init_lock_system()

        self.blk = blk
        self.wal_writer, self.wal_checkpointer = _init_wal_system(self.blk, self.meta)

        if not self.background_proc_disabled:
            self.fork_pg_wal_proc()
        
        self.tx_mgr = _init_transaction_system()
    
    def fork_pg_wal_proc(self):
        th = threading.Thread(target=self.wal_checkpointer.proc)

        self.procs["pg_checkpointer"] = th
        th.start()

        th = threading.Thread(target=self.wal_writer.proc)
        self.procs["pg_wal"] = th
        th.start()

        return th
    
    def terminate(self):
        self.wal_checkpointer.wait_to_terminate()
        exit(0)
    
    def disable_background_proc(self):
        self.background_proc_disabled = True
    
    def start_server(self):
        host         = os.getenv("DB_HOST", "localhost")
        port         = int(os.getenv("DB_PORT", "5678"))
        worker_count = int(os.getenv("DB_WORKERS", "4"))

        asyncio.run(DBServer(app=self, host=host, port=port, worker_count=worker_count).run())


