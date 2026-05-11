import threading

global LAST_TXID
TX_GENERATOR_LOCK = threading.Lock()
LAST_TXID = 0

class TransactionGenerator:
    def __init__(self):
        pass
    
    def next_xid(self):
        global LAST_TXID

        with TX_GENERATOR_LOCK:
            LAST_TXID += 1

        return LAST_TXID
    
    def create(self):
        tx = Transaction()
        tx.xid = self.next_xid()

class Transaction:
    def __init__(self):
        self.xid = None
        self.ref_pages = []
        self.begin_lsn = 0
        self.last_lsn = 0

    def set_xid(self, xid):
        self.xid = xid
