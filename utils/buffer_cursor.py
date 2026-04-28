from utils.dec import *

class buffer_cursor:
    def __init__(self, buffer=None, id=None):
        self.buffer = buffer if buffer is not None else bytearray()
        self.c = 0
        self.id = id
    
    def check(self, length):
        if self.c + length > len(self.buffer):
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
        if length < 0:
            raise Exception("advance length must be greather than zero or equal to zero")
        
        if length == 0:
            return

        self.check(length)
        buf = self.buffer[self.c:self.c + length]
        self.c += length
        self.log(f"cursor advance id={self.id} len={length}; now={self.c}")

    def read_int64(self):
        self.check(8)
        buf = self.buffer[self.c:self.c + 8]
        v = toint64(buf)             
        
        self.read_log(8, v)
        self.c += 8
        return v

    def read_bytes(self):
        length = self.read_int64()
        self.check(length)
        value = self.buffer[self.c:self.c + length]
        self.c += length
        return value
    
    def write_int64_a(self, v):
        self.buffer[self.c:self.c + 8] = serint64(v)
        self.write_log(8, v)
        self.c += 8
    
    def write_int64(self, v):
        self.check(8)
        return self.write_int64_a(v)
    
    def write_varchar(self, string):
        data = string.encode("utf-8")
        length = len(data)
        self.write_int64(length)
        self.check(length)
        self.buffer[self.c:self.c+length] = data
        self.c += length
        return length

    def write_varchar_a(self, string):
        data = string.encode("utf-8")
        length = len(data)
        self.write_int64_a(length)
        self.buffer[self.c:self.c+length] = data
        self.c += length
        return length
        
    def write_bytes_a(self, bytes):
        length = len(bytes)
        self.write_int64_a(length)
        self.buffer[self.c:self.c+length] = bytes
    
    def read_varchar(self):
        length = self.read_int64()
        buf = self.buffer[self.c:self.c+length]
        self.c += length
        return buf.decode("utf-8")
    
    def pad(self, size):
        self.check(size)
        self.buffer[self.c:self.c+size] = b"\x00" * size   
        self.log(f"pad buffer id={self.id} pos={self.c}; len={size}")
        self.c += size
      
    def append(self, buffer):
        self.buffer += buffer
        self.c = len(self.buffer)