from core.const import *
from core.page import is_btree_page, page, ref_page
from core.helper import _buffer, _ptype, _minkey, _id
from utils.buffer_cursor import buffer_cursor

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
            self.keys.insert(0, _minkey(ref_page(self.slots[0])))
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
            return int( _minkey(ref_page(self.slots[0])) < tuple_key )

        idx = 1

        for k in self.keys:
            if tuple_key == k:
                raise Exception(f"duplicated key error {k}")

            if tuple_key < k:
                return idx
            idx += 1
        
        return len(self.slots) - 1
    
    def ptype(self):
        if _ptype(self) == PAGE_TYPE_ROOT:
            return "btree root"
        if _ptype(self) == PAGE_TYPE_INTERNAL:
            return "btree internal"
        if _ptype(self) == PAGE_TYPE_DATA:
            return "btree data"
    
    def update_header_buffer(self):
        assert len(self.slots) > 0
        self.page.update_header_buffer()

        buffer = _buffer(self) 
        c = buffer_cursor(buffer, self.page.id)

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

        cur = buffer_cursor(buffer, id)
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

        cur.advance((MAX_SLOT_COUNT - (key_count + 1)) * 8)

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