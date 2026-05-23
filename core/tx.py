import threading

global LAST_TXID

from core.wal import xlog_begin_transaction, xlog_commit_transaction

global g_transaction_mgr
g_transaction_mgr = None
TX_GENERATOR_LOCK = threading.Lock()
LAST_TXID = 0

class TransactionManager:
    def __init__(self):
        self.active_txs = []
    
    def next_xid(self):
        global LAST_TXID

        with TX_GENERATOR_LOCK:
            LAST_TXID += 1

        return LAST_TXID
    
    def create_xid(self):
        return self.next_xid()
    
    def create_virtual(self):
        tx = Transaction()
        tx.xid = LAST_TXID + 1
    
    def create(self):
        xact = Transaction()
        xact.xid = self.create_xid()

        lsn = xlog_begin_transaction(xact.xid)
        xact.begin_lsn = xact.xid

    def commit(self, xid):
        xact = Transaction()
        xact.xid = self.create_xid()

        lsn = xlog_commit_transaction(xact.xid)

        xact.begin_lsn = xact.xid

        return xact

class Transaction:
    def __init__(self):
        self.xid = None
        self.begin_lsn = 0
        self.last_lsn = 0
        self.commit_lsn = 0

        self.state = None
        self.aborted_flag = None

    def set_xid(self, xid):
        self.xid = xid

def _init_transaction_system():
    global g_transaction_mgr
    assert g_transaction_mgr is None
    g_transaction_mgr = TransactionManager()
    return g_transaction_mgr

def get_transaction_mgr():
    return g_transaction_mgr

def generate_xid():
    return g_transaction_mgr.create_xid()