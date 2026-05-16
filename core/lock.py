from multiprocessing import shared_memory
from utils.dec import serint64, toint64
from utils.logging import info
from os import getpid
import threading

_info = lambda x: info("lock", x)

global MEM_POOL
MEM_POOL = None

NO_LOCK = 0
SHARED_LOCK = 1
EXCLUSIVE_LOCK = 2

MEM_POOL_LOCK = threading.Lock()
MEM_POOL_SIZE = 8192

global LOCK_HELD
LOCK_HELD = {}

global WAITS
WAITS = {}

def _init_lock_system():
    return
    global MEM_POOL
    _info("init lock management system")
    MEM_POOL = shared_memory.SharedMemory("lock_mem_pool", create=True, size=MEM_POOL_SIZE)

class Lock:
    def __init__(self, oid, mode, pid, query=""):
        self.oid = oid
        self.mode = mode
        self.pid = pid
        self.held = False
        self.query = query

    def try_acquire(self):
        with MEM_POOL_LOCK:
            value = get_lock_value(self.oid)

            if possible(value, self.mode):
                set_lock_value(self.oid, self.mode)
                return True
            else:
                return False

    def acquire(self):
        while True:
            res = self.try_acquire()
            if res:
                self.held = True
                assert self.oid not in LOCK_HELD or LOCK_HELD[self.oid] is None
                LOCK_HELD[self.oid] = self

                return res
            else:
                from time import sleep
                sleep(0.05)

    def release(self):
        with MEM_POOL_LOCK:    
            assert self.held
            set_lock_value(self.oid, 0)
            LOCK_HELD[self.oid] = None

def monitor_lock_state():
    for i in range(int(MEM_POOL_SIZE / 8)):
        with MEM_POOL_LOCK:
            buf = MEM_POOL.buf[i * 8: (i + 1) * 8]
            lock_value = toint64(buf)

            if lock_value > 0:
                _info(f"lock held oid={i}, value={lock_value}")

def possible(lock_value, mode):
    if mode == EXCLUSIVE_LOCK:
        return lock_value == 0
    if mode == SHARED_LOCK:
        return lock_value != 1

    raise Exception(f"unknown lock mode {mode}")

def get_lock_value(oid):
    buffer = MEM_POOL.buf[oid * 8: (oid+1) * 8]
    return toint64(buffer)

def set_lock_value(oid, mode):
    MEM_POOL.buf[oid * 8: (oid+1) * 8] = serint64(mode)

