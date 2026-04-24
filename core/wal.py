import threading

LSN_GENERATOR_LOCK = threading.Lock()
LAST_LSN = 0

WAL_BUFFER_RING_SIZE = 1024
WAL_BUFFER_RING = b"\0" * WAL_BUFFER_RING_SIZE

def generate_lsn():
    with LSN_GENERATOR_LOCK:
        LAST_LSN += 1
        return LAST_LSN







def bootstrap_wal_system():
    pass
