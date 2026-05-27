from core.page import page
from core.const import *
from utils.buffer_cursor import buffer_cursor

from utils.logging import info

_info = lambda *x: info("pgextent", *x)

class Extent:
    def __init__(self, entry_page_id, page_num, buffer):
        assert len(buffer) == PAGE_SIZE * page_num
        self.entry_page_id = entry_page_id
        self.page_num = page_num
        self.buffer = buffer

class RotateExtent(Extent):
    ROUND_EXTENT_HEADER_SIZE = 32 # current_page_id, cursor_pos

    def __init__(self, entry_page_id, page_num, buffer):
        super(RotateExtent, self).__init__(entry_page_id, page_num, buffer)
        self.cursor_pos = 0

        self.min_cursor_pos = self.ROUND_EXTENT_HEADER_SIZE
        self.max_cursor_pos = PAGE_SIZE * page_num

    def update_header_buffer(self):
        pass

    def rotation_needed(self, size) -> bool:
        return self.cursor_pos + size + self.ROUND_EXTENT_HEADER_SIZE > self.max_cursor_pos
      
    def rotate(self):
        self.cursor_pos = 0
      
    def get_pos(self):
        return self.ROUND_EXTENT_HEADER_SIZE + self.cursor_pos
    
    def inc(self, size):
        self.cursor_pos += size

    def write_buffer(self, buffer):
        size = len(buffer)
        assert size < PAGE_SIZE * self.page_num

        if self.rotation_needed(size):
            _info(f"rotate at {self.get_pos()}, buffer_size={size}")
            self.rotate()

        pos = self.get_pos()

        cursor = buffer_cursor(self.buffer)
        cursor.at(pos)
        _info(f"write buffer at {pos} buffer_size={size}")

        cursor.write_bytes(buffer)

        self.inc(size)
        
        return pos
