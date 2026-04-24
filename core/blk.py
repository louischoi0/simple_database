from core.const import *
from core.meta import metablock
from core.page import cast_page, page
from core.helper import _id, _buffer
from utils.dec import toint64, serint64

global blk
blk = None

class blk_driver:
    def __init__(self, dev_id):
        self.id = dev_id
        self.f = open(f"file__{dev_id}", "r+b")
        self.cache = {}
       
    def write_page_buffer(self, id, buffer):
        self.f.seek(id * PAGE_SIZE + META_SIZE)
        self.f.write(buffer)
    
    def write_page(self, page):
        page = cast_page(page)

        page.update_header_buffer()
        offset = (_id(page) * PAGE_SIZE) + META_SIZE
        blen = len(_buffer(page))
        self.f.seek(offset)
        self.f.write(_buffer(page))

        print(f"writepage: id={_id(page)}, type={page.ptype()} from={offset}, len={blen}")
    
    def read_page_buffer(self, id):
        self.f.seek((id * PAGE_SIZE) + META_SIZE)
        print(f"read page: from={id*PAGE_SIZE + META_SIZE}, len={PAGE_SIZE}")
        return bytearray(self.f.read(PAGE_SIZE))
    
    def read_page(self, id):
        buffer = self.read_page_buffer(id)
        p = page(*page.parse_header_buffer(buffer))
        p.buffer = buffer
        return p
    
    def init_driver(self):
        self.f.seek(0)
        self.f.write(bytearray(b'\x00' * (MAX_PAGE_COUNT * PAGE_SIZE + META_SIZE)))
        self.commit_metablock(metablock(0))
    
    def read_metablock(self):
        self.f.seek(0)
        meta_buffer = self.f.read(8)
        return metablock(toint64(meta_buffer[:8]))          

    def commit_metablock(self, metablock):
        self.f.seek(0)
        print("commit: ", metablock.max_page)
        self.f.write(serint64(metablock.max_page))          

def _init_blk_driver(dev_id):
    global blk
    blk = blk_driver(dev_id)
    return blk

def get_blk_diver():
    global blk
    return blk