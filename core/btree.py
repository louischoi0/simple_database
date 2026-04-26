from core.const import *
from core.page import is_btree_page, page
from core.page_mgr import ref_page
from core.helper import _buffer, _ptype, _minkey, _id
from core.page_mgr import global_palloc
from core.blk import get_blk_diver
from utils.buffer_cursor import buffer_cursor

class bt_cursor:
    def __init__(self):
        self.ns = []
        self.count = 0
    
    def visit(self, node):
        self.ns.append(node)
    
    def pop(self):
        dest = self.ns[-1]
        self.ns = self.ns[:-1]
        self.count -= 1

        return dest

    def pop_try(self):
        try:
            return self.pop()
        except:
            return None
  
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
    
    def insert_phase_one(self, inode):
        split_node = None

        if not self.is_overflow():
            self.direct_insert(inode)
        else:
            split_node = self.split(inode)

        return split_node

    def insert_phase_zero(self, inode):
        target = self 
        tuple_key = _minkey(inode)
        cursor = bt_cursor()
        split_node = None

        if _ptype(target) == PAGE_TYPE_ROOT:
            cursor.visit(target)

        while True:
            vnode_id = target.get_internal_node_to_go_down(tuple_key)
            vnode = ref_page(vnode_id)
            vnode = bt_node.as_btnode(vnode)

            if _ptype(vnode) != PAGE_TYPE_DATA:
                print(f"visit node for insert {_id(vnode)}")
                cursor.visit(vnode)
                target = vnode
                continue

            if not vnode.is_overflow():
                print(f"direct insert new heap page to {_id(vnode)}")
                vnode.direct_insert(inode)
                return None, cursor
            
            else:
                return vnode.split(inode), cursor

        return None, cursor

    def insert(self, inode):
        split_node, cursor = self.insert_phase_zero(inode)
        target = cursor.pop()

        if split_node is None:
            return self

        while True:
            print(f"pop visit node for split recovery {_id(target)}")

            assert _ptype(target) != PAGE_TYPE_DATA

            split_node = target.insert_phase_one(split_node)

            if split_node is None:
                return self

            ntarget = cursor.pop_try()

            if ntarget is None:
                break
            else:
                target = ntarget

        assert _ptype(target) == PAGE_TYPE_ROOT
        assert split_node is not None

        target.set_page_type(PAGE_TYPE_INTERNAL)
        new_root_right = target.split(split_node)

        new_root_pg = global_palloc()
        new_root_btn = bt_node(PAGE_TYPE_ROOT, target.level + 1, new_right_pg)

        new_root_btn.direct_insert(target)
        new_root_btn.direct_insert(new_root_right)

        new_root_btn.update_header_buffer()
        target.update_header_buffer()
        new_root_right.update_header_buffer()

        return new_root_btn
        
        return self
    
    def set_page_type(self, type):
        self.page.type = type

    def split(self, new_node):
        print(f"split node {_id(self)}")

        index = self.find_leaf_index_to_insert(_minkey(new_node))

        nslots = self.slots.copy()
        nslots.insert(index, _id(new_node))
        nkeys = []

        for node_id in nslots[1:]:
            pg = ref_page(node_id)
            k = _minkey(pg)
            nkeys.append(k)

        i = int(MAX_KEY_COUNT / 2)

        new_right_pg = global_palloc()
        new_right_pg.type = _ptype(self)

        self.keys = nkeys[:i]
        self.slots = nslots[:i+1]
        self.key_count = len(self.keys)
        
        new_right = bt_node(_ptype(self), self.level, new_right_pg)
        new_right.keys = nkeys[i:]
        new_right.slots = nslots[i+1:]
        new_right.key_count = len(new_right.keys)
        new_right.page.min_key = _minkey(ref_page(new_right.slots[0]))
        new_right.update_header_buffer()

        return new_right
    
    def direct_insert(self, inode):
        assert len(self.slots) < MAX_SLOT_COUNT
        assert len(self.slots) == len(self.keys) + 1

        if _ptype(inode) == PAGE_TYPE_HEAP:
            assert _ptype(self) == PAGE_TYPE_DATA

        kindex = self.find_leaf_index_to_insert(_minkey(inode))

        self.slots.insert(kindex, _id(inode))
        nkeys = []

        for node_id in self.slots[1:]:
            pg = ref_page(node_id)
            k = _minkey(pg)
            nkeys.append(k)

        self.keys = nkeys  

        print(f"direct insert: kindex={kindex}, keys={self.keys}, slots={self.slots}")
        self.key_count += 1
        self.update_header_buffer()
    
    def get_internal_node_to_go_down(self, tuple_key):
        idx = 0

        for node_id in self.slots:
            node_pg = ref_page(node_id)
            k = _minkey(node_pg)
            
            if tuple_key == k:
                raise Exception(f"duplicated key error {k}")

            if tuple_key < k:
                return self.slots[idx]
            
            idx += 1
        
        return self.slots[-1]

    def find_leaf_index_to_insert(self, tuple_key):
        idx = 0

        print(f"direct insert: {tuple_key} to {_id(self)},")
        print(self.keys)
        print(self.slots)

        for node_id in self.slots:
            node_pg = ref_page(node_id)
            k = _minkey(node_pg)
            
            if tuple_key == k:
                raise Exception(f"duplicated key error {k}")

            if tuple_key < k:
                return idx
            
            idx += 1
        
        return len(self.slots)
    
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

        if isinstance(page, bt_node):
            return page

        btn = bt_node(_ptype(page), 0,page)
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
        id, type, min_key, level, key_count, keys, slots = self.parse_header_buffer(_buffer(self.page))

        self.page.id = id
        self.page.type = type
        self.page.min_key = min_key

        self.level = level
        self.key_count = key_count
        self.keys = keys
        self.slots = slots
