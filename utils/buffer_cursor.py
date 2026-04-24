from utils.dec import *

class buffer_cursor:
    def __init__(self, buffer=None, id=None):
        self.buffer = buffer if buffer is not None else bytearray()
        self.c = 0
        self.id = id
    
    def check(self, length):
        if self.c + length >= len(self.buffer):
            raise Exception(f"buffer cursor exceeded: {self.c+length} of {len(self.buffer)}")

    def read_log(self, size, v):
        if not BUFFER_CURSOR_DEBUG:
            return

        self.log(f"read buffer id={self.id} pos={self.c}; len={size}; v={v}" )

    def write_log(self, size, v):
        if not BUFFER_CURSOR_DEBUG:
            return

        self.log(f"write buffer id={self.id} pos={self.c}; len={size}; v={v}" )
    
    def log(self, *args):
        if not BUFFER_CURSOR_DEBUG:
            return
        print(*args)
    
    def advance(self, length):
        if length <= 0:
            raise Exception("advance length must be greather than zero")

        self.check(length)
        buf = self.buffer[self.c:self.c + length]
        self.c += length
        self.log(f"cursor advance id={self.id} len={length}; now={self.c}")

        return buf

    def read_int64(self):
        self.check(8)
        buf = self.buffer[self.c:self.c + 8]
        v = toint64(buf)             
        
        self.read_log(8, v)
        self.c += 8
        return v
    
    def write_int64(self, v):
        self.check(8)
        self.buffer[self.c:self.c + 8] = serint64(v)
        self.write_log(8, v)

        self.c += 8

    def append_int64(self, value):
        self.buffer += serint64(value)  
        self.c += 8
    
    def pad(self, size):
        self.check(size)
        self.buffer[self.c:self.c+size] = b"\x00" * size   
        self.log(f"pad buffer id={self.id} pos={self.c}; len={size}")
        self.c += size
      
    def append(self, buffer):
        self.buffer += buffer
        self.c = len(self.buffer)