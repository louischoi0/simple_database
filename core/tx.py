import threading
from enum import Enum
from dataclasses import dataclass, field
from core.meta import get_metablock
from core.heap import StructuredTuple, heap_page as HeapPage

from core.const import PAGE_SIZE
from utils import buffer_cursor
from utils.logging import info

global LAST_TXID

from core.wal import xlog_begin_transaction, xlog_commit_transaction
from utils.dec import serbit, tobit

global g_transaction_mgr
g_transaction_mgr = None
TX_GENERATOR_LOCK = threading.Lock()
LAST_TXID = 0

_info = lambda *x: info("txmgr", *x)

UNDO_LOGS = {}

class UndoEntry:
    def __init__(self, owner_oid, pk, xmin, operation, old_tuple_buffer, prev=None):
        self.owner_oid = owner_oid
        self.pk = pk
        self.xmin = xmin
        self.operation = operation
        self.old_tuple_buffer = old_tuple_buffer
        self.prev = prev

class TxStatus(Enum):
    IN_PROGRESS = "in_progress"
    COMMITTED = "committed"
    ABORTED = "aborted"
    ERROR = "error"

    @classmethod
    def value(cls, value):
        if value == TxStatus.IN_PROGRESS:
            return 2
        elif value == TxStatus.COMMITTED:
            return 1
        elif value == TxStatus.ABORTED:
            return 0
        elif value == TxStatus.ERROR:
            return 3
    
    @classmethod
    def from_buffer(cls, buffer):
        return tobit(buffer)

class Transaction:
    def __init__(self):
        self.xid = None
        self.begin_lsn = 0
        self.last_lsn = 0
        self.commit_lsn = 0
        self.status = None
        self.aborted_flag = None

    def set_xid(self, xid):
        self.xid = xid

    def mark_error(self):
        self.status = TxStatus.ERROR
    
    def is_error(self):
        return self.status == TxStatus.ERROR

    def execute(self, operation):
        if self.is_error():
            return
    
class UndoFreeHeapSpaceMap:

    def __init__(self, alloc):
        self.free_spaces_map = {}
        self.free_spaces = []
        self.alloc = alloc

    def grow(self, page_num):
        for _ in range(page_num):
            new_pg = self.alloc.halloc()

            self.free_spaces_map[new_pg.id] = new_pg
            self.free_spaces.append(new_pg)

    def reserve(self, size):
        for page in self.free_spaces:
            if page.possible(size):
                page.acquire_lock()
                return page
        return None

class TransactionManager:
    def __init__(self, alloc):
        self.active_txs = []
        self.active_txs_hash = {}
        self.clog_pages = []

        self.current_clog_page = None
        self.metablock = get_metablock()
        self.undo_log_lock = threading.Lock()

        self.undo_fsm = UndoFreeHeapSpaceMap(alloc)
    
    def get_undo_logs(self):
        return UNDO_LOGS
    
    def append_undo_log(self, new_tuple: StructuredTuple, undo, owner_oid, pk):
        with self.undo_log_lock:
            if owner_oid not in UNDO_LOGS:
                UNDO_LOGS[owner_oid] = {}
            
            if pk not in UNDO_LOGS[owner_oid]:
                UNDO_LOGS[owner_oid] = []

            xmin, xmax = StructuredTuple.get_minmax_from_buffer(undo.old_tuple_buffer)
            UNDO_LOGS[owner_oid][pk].append( ( (xmin, xmax),  undo))
        
        size = len(undo.old_tuple_buffer)
        page: HeapPage = self.undo_fsm.reserve()

        assert page is not None

        index = page.write_buffer(undo.old_tuple_buffer)
        assert index >= 0

        new_tuple.set_undoptr(page.id, index)

    #def xlog_write_undo_log(self, ):
    
    def write_transaction_status_flag(self, xid, status):
        page_id = xid // PAGE_SIZE
        offset = xid % PAGE_SIZE

        page = self.clog_pages[page_id]

        cursor = buffer_cursor(page.buffer)
        cursor.at(offset)

        cursor.write_bit(status == TxStatus.COMMITTED)
    
    def next_xid(self):
        global LAST_TXID

        with TX_GENERATOR_LOCK:
            LAST_TXID += 1

        self.metablock.set_oldest_xid_with_commit(LAST_TXID)

        return LAST_TXID
    
    def create_xid(self):
        return self.next_xid()
    
    def create_virtual(self):
        tx = Transaction()
        tx.xid = LAST_TXID + 1
    
    def on_create_xact(self, xact):
        assert xact.status == TxStatus.IN_PROGRESS
        assert xact.xid not in self.active_txs_hash

        self.active_txs.append(xact)
        self.active_txs_hash[xact.xid] = xact

    def get_xact(self, xid) -> Transaction:
        return self.active_txs_hash[xid]

    def create(self):
        xact = Transaction()
        xact.xid = self.create_xid()

        #lsn = xlog_begin_transaction(xact.xid)
        #xact.begin_lsn = lsn

        return xact

    def commit(self, xid):
        xact = Transaction()
        xact.xid = self.create_xid()

        lsn = xlog_commit_transaction(xact.xid)
        self.write_transaction_status_flag(xid, TxStatus.COMMITTED)

        xact.commit_lsn = lsn
        return xact

def _init_transaction_system():
    global g_transaction_mgr
    global LAST_TXID

    assert g_transaction_mgr is None

    g_transaction_mgr = TransactionManager()
    LAST_TXID = g_transaction_mgr.metablock.oldest_xid
    _info(f"last txid: {LAST_TXID}")

    return g_transaction_mgr

def get_transaction_mgr():
    return g_transaction_mgr

def generate_xid():
    return g_transaction_mgr.create_xid()
