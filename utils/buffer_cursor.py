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
    
    def at(self, pos):
        self.c = pos
    
    def advance(self, length):
        if length < 0:
            raise Exception("advance length must be greather than zero or equal to zero")
        
        if length == 0:
            return

        self.check(length)
        buf = self.buffer[self.c:self.c + length]
        self.c += length
        self.log(f"cursor advance id={self.id} len={length}; now={self.c}")
        return buf
    
    def read(self, size):
        self.check(size)
        buf = self.buffer[self.c: self.c+size]
        self.c += size
        return buf
    
    def read_int32(self):
        self.check(4)
        buf = self.buffer[self.c:self.c + 4]
        v = toint32(buf)             
        
        self.read_log(4, v)
        self.c += 4
        return v

    def read_int64(self):
        self.check(8)
        buf = self.buffer[self.c:self.c + 8]
        v = toint64(buf)             
        
        self.read_log(8, v)
        self.c += 8
        return v

    def read_bytes(self):
        length = self.read_int64()

        if length == 0:
            return None

        self.check(length)
        value = self.buffer[self.c:self.c + length]
        self.c += length
        return value

    def write_int32_a(self, v):
        self.buffer[self.c:self.c + 4] = serint32(v)
        self.write_log(4, v)
        self.c += 4
    
    def write_int32(self, v):
        self.check(4)
        return self.write_int32_a(v)
    
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
        
    def write_bytes_a(self, buffer):
        if buffer is None:
            return self.write_int64_a(0)

        length = len(buffer)
        self.write_int64_a(length)
        self.buffer[self.c:self.c+length] = buffer
        self.c += length

    def write_bytes(self, buffer):
        if buffer is None:
            return self.write_int64(0)

        length = len(buffer)
        self.check(length + 8)
        self.write_int64(length)
        return self.write_raw(buffer)
    
    def write_raw(self, buffer):
        # write_raw write buffer without size prefix
        length = len(buffer)
        self.check(length)
        self.buffer[self.c:self.c+length] = buffer
        self.c += length
    
    def read_varchar(self):
        length = self.read_int64()
        buf = self.buffer[self.c:self.c+length]
        self.c += length
        return buf.decode("utf-8")

    def read_char(self, length):
        self.check(length)
        buf = self.buffer[self.c:self.c+length]
        self.c += length
        return buf.decode("utf-8")

    def write_char(self, value, length):
        self.check(length)
        self.buffer[self.c: self.c + length] = value.encode("utf-8")
        self.c += length

    def write_char_a(self, value, length):
        self.buffer[self.c: self.c + length] = value.encode("utf-8")
        self.c += length
    
    def pad(self, size):
        self.check(size)
        return self.pad_a(size)

    def pad_a(self, size):
        self.buffer[self.c:self.c+size] = b"\x00" * size   
        self.log(f"pad buffer id={self.id} pos={self.c}; len={size}")
        self.c += size
        return size      

    def append(self, buffer):
        self.buffer += buffer
        self.c = len(self.buffer)
    
    def end(self):
        return self.c == len(self.buffer)
    
    def tail(self):
        return self.buffer[self.c:]

    def write_bool_a(self, value):
        value = int(value)
        data = serbit(value)
        self.buffer[self.c:self.c+1] = data
        self.c += 1
    
    def write_bool(self, value):
        value = int(value)
        data = serbit(value)
        self.check(1)
        self.buffer[self.c:self.c+1] = data
        self.c += 1
    
    def read_bool(self):
        self.check(1)
        v = self.buffer[self.c:self.c+1]
        self.c += 1
        return tobit(v)

    def write_dynamic_type_a(self, type_val, value, size=None):
        from core.catalog import get_type_value

        if type_val == get_type_value("int"):
            return self.write_int64_a(value)
            
        elif type_val == get_type_value("bool"):
            return self.write_bool_a(value)
            
        elif type_val == get_type_value("varchar"):
            return self.write_varchar_a(value)
                
        elif type_val == get_type_value("char"):
            assert size is not None
            return self.write_char_a(value, size)

        elif type_val == get_type_value("bytes"):
            return self.write_bytes_a(value)

        else :
            raise Exception(f"invalid type value {type_val}")
    
    def read_dynamic_type_a(self, type_val, size=None):
        from core.catalog import get_type_value

        if type_val == get_type_value("int"):
            return self.read_int64()
        elif type_val == get_type_value("bool"):
            return self.read_bool()
            
        elif type_val == get_type_value("varchar"):
            return self.read_varchar()
                
        elif type_val == get_type_value("char"):
            return self.read_char(size)

        elif type_val == get_type_value("bytes"):
            return self.read_bytes()

        else :
            raise Exception(f"invalid type value {type_val}")