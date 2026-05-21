import threading
from enum import Enum
from dataclasses import dataclass, field

global LAST_TXID
TX_GENERATOR_LOCK = threading.Lock()
LAST_TXID = 0

class TxStatus(Enum):
    IN_PROGRESS = "in_progress"
    COMMITTED = "committed"
    ABORTED = "aborted"

class Transaction:
    xid = None
    status: TxStatus = TxStatus.IN_PROGRESS
    undo_log: list
    write_log: list

    write_set: set
    read_set: set

    def add_write_set(self, xlog):
        self.write_log.append(xlog)

class TransactionManager:
    def __init__(self):
        self.active_txs = []
        self.active_txs_hash = {}
    
    def next_xid(self):
        global LAST_TXID

        with TX_GENERATOR_LOCK:
            LAST_TXID += 1

        return LAST_TXID
    
    def create(self):
        tx = Transaction()
        tx.xid = self.next_xid()
    
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

    def on_insert_xlog(self, xlog):
        tx: Transaction = self.get_xact(xlog.xid) 
        tx.add_write_set(xlog)
