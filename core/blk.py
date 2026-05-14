from core.const import *
from core.meta import metablock
from core.page import cast_page, page
from core.helper import _id, _buffer, _checksum
from utils.logging import info
from utils.dec import toint64, serint64
import threading

global blk
blk = None
_info = lambda x: info("blk", x)

class blk_driver:
    def __init__(self, dev_id):
        assert blk is None

        self.id = dev_id
        self.f = open(f"file__{dev_id}", "r+b")
        self.lock = threading.Lock()
       
    def write_page_buffer(self, id, buffer):
        self.f.seek(id * PAGE_SIZE + META_SIZE)
        self.f.write(buffer)
    
    def write_page(self, page):
        page = cast_page(page)

        page.update_header_buffer()
        offset = (_id(page) * PAGE_SIZE) + META_SIZE
        blen = len(_buffer(page))

        with self.lock:
            self.f.seek(offset)
            self.f.write(_buffer(page))
            self.f.flush()

        _info(f"writepage {self.id}: id={_id(page)}, type={page.ptype()} from={offset}, len={blen}, checksum={_checksum(page)()}")

    def read_page_buffer(self, id):
        self.f.seek((id * PAGE_SIZE) + META_SIZE)
        _info(f"read page {self.id}: id={id}, from={id*PAGE_SIZE + META_SIZE}, len={PAGE_SIZE} ")
        return bytearray(self.f.read(PAGE_SIZE))
    
    def read_page(self, id):
        with self.lock:
            buffer = self.read_page_buffer(id)
            p = page(*page.parse_header_buffer(buffer))
            p.buffer = buffer
            _info(f"page {id} checksum: {p.checksum()}")
            return p
    
    def init_driver(self):
        self.f.seek(0)
        self.f.write(bytearray(b'\x00' * (MAX_PAGE_COUNT * PAGE_SIZE + META_SIZE)))
        self.f.flush()
    
def _init_blk_driver(dev_id):
    global blk
    blk = blk_driver(dev_id)
    return blk

def get_blk_diver():
    global blk
    return blk