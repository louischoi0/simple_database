import sys

PAGE_SIZE = 1024 * 8
BYTE_ORDER = "little"
HDR_SIZE = 24
MAX_PAGE_COUNT = 256
META_SIZE = 256

PAGE_TYPE_ROOT = 0 
PAGE_TYPE_INTERNAL = 1
PAGE_TYPE_DATA = 2
PAGE_TYPE_HEAP = 4

MAX_KEY_COUNT = 3
MAX_SLOT_COUNT = MAX_KEY_COUNT + 1
BTREE_DETAIL_HDR_SIZE = 8 + 8 + (8*(MAX_SLOT_COUNT-1)) + (8 * MAX_SLOT_COUNT)

BUFFER_CURSOR_DEBUG = True


global blk
global alloc

def serint64(value: int) -> bytes:
    return value.to_bytes(8, byteorder=BYTE_ORDER, signed=True)

def toint64(buf: bytes) -> int:
    return int.from_bytes(buf, byteorder=BYTE_ORDER, signed=True)

def is_heap_page(page):
    if hasattr(page, "page"):
        return page.page.type == PAGE_TYPE_HEAP
    else:
        return page.type == PAGE_TYPE_HEAP
    
def is_btree_page(page):
    if hasattr(page, "page"):
        return page.page.type in (PAGE_TYPE_DATA, PAGE_TYPE_INTERNAL, PAGE_TYPE_ROOT, )
    else:
        return page.type in (PAGE_TYPE_DATA, PAGE_TYPE_INTERNAL, PAGE_TYPE_ROOT, )

def _minkey(page):
    if hasattr(page, "page"):
        return page.page.min_key
    return page.min_key

def _id(page):
    if hasattr(page, "page"):
        return page.page.id
    return page.id

def _buffer(page):
    if hasattr(page, "page"):
        return page.page.buffer
    return page.buffer

def cast_page(page):
    if is_btree_page(page):
        return bt_node.as_btnode(page)
    
    if is_heap_page(page):
        return page.as_heap()

def check_valid_header_size(page):
    assert len(_buffer(page)) == HDR_SIZE

class metablock:
    def __init__(self, max_page):
        self.max_page = max_page
    
    def set_max_page(self, max_page):
        self.max_page = max_page
    
    def inc(self):
        self.max_page += 1
        return self.max_page

def ref_page(id):
    try:
        return blk.cache[id]
    except KeyError:
        page = blk.read_page(id)
        blk.cache[id] = page
        return page

    return None

class blk_driver:
    def __init__(self, dev_id):
        self.id = dev_id
        self.f = open(f"file__{dev_id}", "r+b")
        self.cache = {}
      
    def commit_all_pages(self):
        for id in blk.cache:
            page = blk.cache[id]
            blk.write_page(page)
    
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

        print(f"writepage: id={_id(page)}, type={type(page)} from={offset}, len={blen}")
    
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


class page:
    def __init__(self, page_id, type, min_key):
        self.id = page_id
        self.min_key = min_key
        self.type = type
        self.buffer = bytearray(b'\x00' * int(PAGE_SIZE))
    
    def update_header_buffer(self):
        header_buffer = self.ser_header()
        assert len(header_buffer) == HDR_SIZE
        self.buffer[:len(header_buffer)] = header_buffer
    
    def ser_header(self):
        print("page ser header")
        return (
            serint64(self.id) +
            serint64(self.type) +
            serint64(self.min_key)
        )

    @classmethod
    def parse_header_buffer(cls, buffer):
        return (
            toint64(buffer[0:8]),
            toint64(buffer[8:16]),
            toint64(buffer[16:24]),
        )
    
    def apply_header_buffer(self):
        id, type, min_key = self.parse_header_buffer(self.buffer)
        self.id = id
        self.type = type
        self.min_key
    
    def as_heap(self):
        assert self.type == PAGE_TYPE_HEAP
        p = heap_page(self.id)
        p.buffer = self.buffer
        return p
    
class tuple:
    def __init__(self, value):
        self.value = value
    
    def size(self):
        return 8
    
    def ser(self):
        return serint64(self.value)
    
    @classmethod
    def parse(self, buffer):
        return toint64(buffer)

class heap_page(page):
    def __init__(self, page_id):
        super().__init__(page_id, type, -1)
        self.tuple_count = 0
        self.type = PAGE_TYPE_HEAP
    
    def insert(self, t):
        self.buffer[HDR_SIZE:HDR_SIZE+8] = serint64(self.tuple_count)
        data_offset = HDR_SIZE + (self.tuple_count * 8) + 8

        assert len(t.ser()) == 8
        self.buffer[data_offset:data_offset+t.size()] = t.ser()

        assert t.value == tuple.parse(t.ser())

        self.tuple_count += 1
    
    def iter(self, f):
        read_cursor = buffer_cursor(self.buffer)
        read_cursor.advance(HDR_SIZE)
        _tuple_count = read_cursor.read_int64()

        assert _tuple_count == self.tuple_count

        for _ in range(self.tuple_count):
            value = read_cursor.read_int64()
            f(value)

    def apply_header_buffer(self):
        id, type, min_key = self.parse_header_buffer(self.buffer)
        c = buffer_cursor(self.buffer)
        c.advance(HDR_SIZE)

        self.id = id
        self.type = type
        self.min_key = min_key
        self.tuple_count = c.read_int64()

def new_data_page_ini(allocator, min_key):
    new_page = allocator.palloc()
    new_page.type = PAGE_TYPE_DATA
    new_page.min_key = min_key
    new_page.update_header_buffer()

    btn = bt_node(PAGE_TYPE_DATA, 0, new_page)

    nhpage = new_heap_page()

    nhpage.min_key = min_key
    nhpage.insert(tuple(min_key))

    btn.slots = [nhpage.id]
    btn.keys = []

    blk.write_page(nhpage)

    return btn

def new_root_page(allocator, min_key):
    new_root_page = allocator.palloc()
    new_root_page.type = PAGE_TYPE_ROOT

    btn = bt_node(PAGE_TYPE_ROOT, 1, new_root_page)
    data_page = new_data_page_ini(alloc, min_key)
    btn.slots = [data_page.page.id]
    blk.write_page(data_page.page)

    return btn

class buffer_cursor:
    def __init__(self, buffer=None):
        self.buffer = buffer if buffer is not None else bytearray()
        self.c = 0

    def read_log(self, size, v):
        if not BUFFER_CURSOR_DEBUG:
            return

        print(f"read buffer pos={self.c}; len={size}; v={v}" )
    def read_log(self, size, v):
        if not BUFFER_CURSOR_DEBUG:
            return

        print(f"read buffer pos={self.c}; len={size}; v={v}" )
    
    def advance(self, length):
        buf = self.buffer[self.c:self.c + length]
        self.c += length
        print(f"cursor advance len={length}; now={self.c}")

        return buf

    def read_int64(self):
        buf = self.buffer[self.c:self.c + 8]
        v = toint64(buf)             
        
        self.read_log(8, v)
        self.c += 8
        return v
    
    def write_int64(self, v):
        self.buffer[self.c:self.c + 8] = serint64(v)
        self.c += 8

    def append_int64(self, value):
        self.buffer += serint64(value)  
        self.c += 8
    
    def pad(self, size):
        self.buffer += b"\x00" * size   
        self.c += size
      
    def append(self, buffer):
        self.buffer += buffer
        self.c = len(self.buffer)

class bt_cursor:
    def __init__(self):
        self.ns = []
        self.count = 0
    
    def visit(self, node):
        self.ns.append(node)
    
    def back(self):
        dest = self.ns[-1]
        self.ns = self.ns[:-1]
        self.count -= 1

        return dest
  
class bt_node:
    def __init__(self, type, level, page):
        self.key_count = 0
        self.keys = []
        self.slots = []
        self.level = level
        self.page = page
    
    def is_overflow(self):
        assert len(self.keys) <= MAX_KEY_COUNT
        return len(self.keys) == MAX_KEY_COUNT
    
    def is_underflow(self):
        assert len(self.keys) <= MAX_KEY_COUNT
        return len(self.keys) < MAX_KEY_COUNT / 2
      
    def split(self, add):
        assert vnode.is_overflow()

        min_key = add.page.min_key
        index = self.find_internal_index_to_insert()

        new_keys = self.keys.copy()
        new_slots = self.slots.copy()

        new_slots.insert(min_key, index)
        new_btn = new_bt_data()
    
    def insert(self, inode):
        print(self.page.type)
        if not self.is_overflow():
            return self.direct_insert(inode)

        target = self 
        tuple_key = _minkey(inode)
        cursor = bt_cursor()

        while True:
            vnode_id = target.find_internal_node_to_insert(min_key)
            print(f"internal node: {vnode_id}")
            vnode = ref_page(vnode_id)

            cursor.visit(vnode)

            if not vnode.is_overflow():
                return vnode.direct_insert(inode)
            
            if vnode.is_overflow() and vnode.level == 0:
                pass

            target = vnode
    
    def direct_insert(self, inode):
        assert len(self.slots) < MAX_SLOT_COUNT

        kindex = self.find_internal_index_to_insert(_minkey(inode))

        if kindex == 0:
            self.keys.insert(0, _minkey(rfp(self.slots[0])))
        else:
            self.keys.insert(kindex, _minkey(inode))

        print(f"direct insert: kindex={kindex}, keys={self.keys}")
        self.slots.insert(kindex, _id(inode))
        self.key_count += 1

    def find_internal_node_to_insert(self, tuple_key):
        idx = self.find_internal_index_to_insert(tuple_key)
        return self.slots[idx]
    
    def find_internal_index_to_insert(self, tuple_key):
        if len(self.keys) == 0:
            return int( _minkey(rfp(self.slots[0])) < tuple_key )

        idx = 1

        for k in self.keys:
            if tuple_key == k:
                raise Exception(f"duplicated key error {k}")

            if tuple_key < k:
                return idx
            idx += 1
        
        return len(self.slots) - 1
    
    def update_header_buffer(self):
        assert len(self.slots) > 0
        self.page.update_header_buffer()

        buffer = _buffer(self) 
        c = buffer_cursor(buffer)

        c.advance(HDR_SIZE)

        c.write_int64(self.level)
        c.write_int64(self.key_count)

        for k in self.keys:
            c.write_int64(k)
        
        c.pad( (MAX_KEY_COUNT - len(self.keys)) * 8 )
      
        for s in self.slots:
            c.write_int64(s)

        c.pad( (MAX_KEY_COUNT + 1 - len(self.keys)) * 8 )
        
        assert len(c.buffer) == len(self.page.buffer)
        self.page.buffer = c.buffer

    @classmethod
    def as_btnode(cls, page):
        assert is_btree_page(page)

        btn = bt_node(page.type, 0,page)
        btn.apply_header_buffer()
        return btn

    @classmethod
    def parse_header_buffer(cls, buffer):
        id, type, min_key = page.parse_header_buffer(buffer)

        cur = buffer_cursor(buffer)
        cur.advance(HDR_SIZE)

        level = cur.read_int64()
        key_count = cur.read_int64()
        keys = []
        slots = []

        for _ in range(key_count):
            keys.append(cur.read_int64())
        
        cur.advance((MAX_KEY_COUNT - key_count) * 8)

        for _ in range(key_count + 1):
            page_id = cur.read_int64()
            slots.append(page_id)

        cur.advance((MAX_SLOT_COUNT - ((key_count + 1)) * 8))

        return id, type, min_key, level, key_count, keys, slots

    def apply_header_buffer(self):
        id, type, min_key, level, key_count, keys, slots = self.parse_header_buffer(self.page.buffer)

        self.page.id = id
        self.page.type = type
        self.page.min_key = min_key

        self.level = level
        self.key_count = key_count
        self.keys = keys
        self.slots = slots
    
def new_heap_page():
    heap_page = alloc.hpalloc()
    return heap_page

def new_bt_data(key):
    npage = alloc.palloc()
    bt_node = bt_node(key, PAGE_TYPE_DATA, 0, npage)

    bt_node.keys = []
    bt_node.key_count = 0
    bt_node.slots = [key]

    return bt_node

def check_btree_page(bpage):
    header_buffer = bpage.page.ser_header()
    id, type, r_min_key, level, key_count, keys, slots = bpage.parse_header_buffer(header_buffer)

    print("keys: ", bpage.keys)
    print("slots: ", bpage.slots)
    print("key_count: ", bpage.key_count)

    assert id == bpage.page.id
    assert type == bpage.page.type
    assert r_min_key == bpage.page.min_key
    assert bpage.slots == slots
    assert bpage.key_count == key_count

def exec_command(cmd):
    ctype = cmd[0]
    print("exec: ", ctype)

    if ctype == "init":
        blk.init_driver()
    
    elif ctype == "new_root":
        min_key = int(cmd[1])
        root_page = new_root_page(alloc, min_key)

        print(f"root page: {root_page.page.id}; slots: {root_page.slots}")

        root_page.update_header_buffer()
        check_btree_page(root_page)

        blk.write_page(root_page.page)
        blk.commit_metablock(alloc.metablock)
    
    elif ctype == "insert_bt":
        root_page_id = int(cmd[1])
        new_key = int(cmd[2])

        page = blk.read_page(root_page_id)
        btn = bt_node.as_btnode(page)

        h = new_heap_page()
        h.insert(tuple(new_key))

        blk.write_page(h)
        btn.insert(h)
    
    elif ctype == "read_bt":
        page_id = int(cmd[1])

        page = blk.read_page(page_id)
        btn = bt_node.as_btnode(page)

        check_btree_page(btn)
    
    elif ctype == "test":
        h = new_heap_page()
        h.id = 9
        h.min_key = 11
        h.tuple_count = 2
        buffer = h.ser_header()
        a, b, c, d = heap_page.parse_header_buffer(buffer)
        print(a,b,c,d)      

    elif ctype == "new_heap":
        h = new_heap_page()
        blk.write_page(h)
        print("heap page allocated: ", h.id, h.type)

        blk.commit_metablock(alloc.metablock)

    elif ctype == "insert":
        page_id = int(cmd[1])
        value = int(cmd[2])

        page = blk.read_page(page_id)
        hpage = page.as_heap()
        hpage.apply_header_buffer()

        assert hpage.type == PAGE_TYPE_HEAP
        assert hpage.id == page_id

        hpage.insert(tuple(value))

        blk.write_page(hpage)
        blk.commit_metablock(alloc.metablock)
    
    elif ctype == "iter":
        page_id = int(cmd[1])

        page = blk.read_page(page_id)

        assert page.type == PAGE_TYPE_HEAP
        assert page.id == page_id

        hpage = page.as_heap()
        hpage.apply_header_buffer()
        print("iter page %d (%d k)" % (hpage.id, hpage.tuple_count))
        hpage.iter(print)
        blk.commit_metablock(alloc.metablock)
    
    else: 
        raise Exception("unknown command type: ", ctype)

if __name__ == "__main__":
    blk = blk_driver(0)
    alloc = page_allocator(blk)

    exec_command(sys.argv[1:])

    blk.commit_all_pages()

    if sys.argv[1] != "init":
        blk.commit_metablock(alloc.metablock)