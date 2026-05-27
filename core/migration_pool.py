from threading import Lock
from core.extent import RotateExtent
from core.heap import HeapTuple
from page_mgr import page_allocator

global _g_migration_pool
_g_migration_pool = None

class MigrationPool:
    def __init__(self, extent: RotateExtent):
        self.lock = Lock()
        self.extent = extent

        # (owner_oid, pk)
        self.pool = {}
        self.tuple_pos = {}
        self.live_border = 0

    def get(self, owner_oid, pk):
        with self.lock:
            return self.pool.get((owner_oid, pk), None)

    def migrate(self, owner_oid, pk, heap_tuple):
        with self.lock:
            self.pool[(owner_oid, pk)] = heap_tuple
            self.callback(owner_oid, pk, heap_tuple)
    
    def callback(self, owner_oid, pk, heap_tuple: HeapTuple):
        self.tuple_pos[(owner_oid, pk)] = self.extent.write_buffer(heap_tuple.buffer)
    
    def clear_tuple(self, owner_oid, pk):
        with self.lock:
            del self.pool[(owner_oid, pk)]
            del self.tuple_pos[(owner_oid, pk)]

def _init_migration_pool(allocator: page_allocator):
    global _g_migration_pool
    extent = allocator.alloc_sys_rotate_extent(64)
    _g_migration_pool = MigrationPool(extent)

    return _g_migration_pool