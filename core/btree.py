from core.const import *
from core.page import is_btree_page, page, is_heap_page, is_btree_data_page
from core.page_mgr import ref_page, ref_minkey
from core.helper import _buffer, _ptype, _minkey, _id
from core.page_mgr import global_palloc
from core.blk import get_blk_diver
from core.wal import xlog_full_page_write

from utils.buffer_cursor import buffer_cursor
from utils.logging import info

_info = lambda x: info("btree", x)

class bt_cursor:
    def __init__(self):
        self.ns = []
        self.count = 0
    
    def size(self):
        return len(self.ns)
    
    def visit(self, node):
        self.ns.append(node)
        self.count += 1
    
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
        self.next_page_id = 0
        self.page = page
        self.page.type = type
    
    def set_next_page_id(self, next):
        self.next_page_id = next
    
    def is_overflow(self):
        assert len(self.keys) <= MAX_KEY_COUNT
        return len(self.keys) == MAX_KEY_COUNT
    
    def is_underflow(self):
        assert len(self.keys) <= MAX_KEY_COUNT
        return len(self.keys) < MAX_KEY_COUNT / 2
    
    def insert_phase_one(self, inode):
        split_node = None
        insert_index = -1

        if not self.is_overflow():
            insert_index = self.direct_insert(inode)
        else:
            split_node = self.split(inode)

        return split_node, insert_index
    
    @classmethod
    def new_root_page(cls, allocator):
        new_root_page = allocator.palloc(PAGE_TYPE_ROOT)
        new_root_page.type = PAGE_TYPE_ROOT
        new_root_page.min_key = -1

        return bt_node(PAGE_TYPE_ROOT, 1, new_root_page)
    
    def empty(self):
        return len(self.slots) == 0

    def insert_tuple_with_init(self, alloc, tuple, ctx=None):
        assert self.empty()

        with self.page.lock:
            self.min_key = tuple.pk

            new_data_page = alloc.palloc()
            new_data_page.type = PAGE_TYPE_DATA
            new_data_page.min_key = tuple.pk

            btn = bt_node(PAGE_TYPE_DATA, 0, new_data_page)

            nhpage = alloc.hpalloc()
            nhpage.min_key = tuple.pk
            nhpage.insert(tuple, ctx=ctx)

            btn.slots = [nhpage.id]
            btn.keys = []

            self.slots = [ _id(new_data_page), ]
            self.key_count = 0
            self.page.min_key = tuple.pk

            self.update_header_buffer()
            btn.update_header_buffer()
            nhpage.update_header_buffer()

            if ctx is not None:
                xlog_full_page_write(ctx.wal_writer, ctx.xid, self)
                xlog_full_page_write(ctx.wal_writer,ctx.xid, btn)
                xlog_full_page_write(ctx.wal_writer,ctx.xid, nhpage)

                xlog0 = bt_node.create_xlog_btree_slot_insert(ctx.xid, _id(self), _id(btn), 0)
                xlog1 = bt_node.create_xlog_btree_slot_insert(ctx.xid, _id(btn), _id(nhpage), 0)

                ctx.wal_writer.write_xlog(xlog0)
                ctx.wal_writer.write_xlog(xlog1)

    def insert_phase_zero(self, inode, ctx=None):
        target = self 
        tuple_key = _minkey(inode)
        cursor = bt_cursor()

        if _ptype(target) == PAGE_TYPE_ROOT:
            cursor.visit(target)

        while True:
            vnode_id = target.get_internal_node_to_go_down(tuple_key)
            vnode = ref_page(vnode_id)

            # rechaed end of nodes in btree data nodes
            if not is_btree_page(vnode):
                cursor.visit(target)
                return target.split(inode), cursor, -1

            vnode = bt_node.as_btnode(vnode)

            if _ptype(vnode) != PAGE_TYPE_DATA:
                _info(f"visit node for insert {_id(vnode)}")
                cursor.visit(vnode)
                target = vnode
                continue

            if not vnode.is_overflow():
                _info(f"direct insert new heap page{_id(inode)} to {_id(vnode)}")
                index = vnode.direct_insert(inode, ctx=ctx)
                return None, cursor, index
            
            else:
                return vnode.split(inode), cursor, -1

    def update_min_key_upper_nodes(self, min_key, cursor):
        node = cursor.pop_try()
        while node is not None:
            node.set_min_key(min_key)
            node = cursor.pop_try()
    
    def validate(self):
        self.update_header_buffer()
        assert is_btree_page(self)

        id, type, r_min_key, level, key_count, keys, slots, next_page_id = bt_node.parse_header_buffer(_buffer(self))
        assert self.page.type == type

        if self.empty():
            assert len(self.keys) == 0 and len(keys) == 0
        else:
            assert len(self.keys) + 1 == len(self.slots) and len(keys) + 1 == len(slots)

        assert id == self.page.id
        assert type == self.page.type
        assert r_min_key == self.page.min_key
        assert self.slots == slots
        assert self.key_count == key_count
        assert self.next_page_id == next_page_id
        assert self.keys == sorted(self.keys)
        assert keys == sorted(keys)
        assert keys == self.keys
        assert self.level == level
    
    @classmethod
    def merge_split_node(cls, insert_min_key, cursor, split_node, ctx=None):
        target = cursor.pop()
        node_type_before_split = None

        while True:
            _info(f"pop visit node for split recovery id={_id(target)}, ptype={_ptype(target)}")
            assert _ptype(target) != PAGE_TYPE_DATA

            node_type_before_split = _ptype(target)
            split_node, insert_index = target.insert_phase_one(split_node)

            if split_node is None:
                if insert_index == 0:
                    target.update_min_key_upper_nodes(insert_min_key)
                return target

            ntarget = cursor.pop_try()

            if ntarget is None:
                break
            else:
                target = ntarget

        assert node_type_before_split == PAGE_TYPE_ROOT
        assert split_node is not None

        new_root_pg = global_palloc()
        new_root_btn = bt_node(PAGE_TYPE_ROOT, target.level + 1, new_root_pg)

        new_root_btn.keys = [ _minkey(split_node) ]
        new_root_btn.slots = [ _id(target), _id(split_node) ]
        new_root_btn.page.min_key = _minkey(target)
        new_root_btn.level = target.level + 1
        new_root_btn.key_count = 1

        new_root_btn.update_header_buffer()
        target.update_header_buffer()

        return new_root_btn

    def insert(self, inode, ctx=None):
        assert _ptype(self) == PAGE_TYPE_ROOT

        split_node, cursor, insert_index = self.insert_phase_zero(inode, ctx=ctx)
        insert_min_key = _minkey(inode)

        if split_node is None:
            if insert_index == 0:
                self.update_min_key_upper_nodes(insert_min_key, cursor)
            return self

        return bt_node.merge_split_node(insert_min_key, cursor, split_node)
        
    def set_page_type(self, type):
        self.page.type = type

    def split(self, new_node):
        _info(f"split node {_id(self)}:{_ptype(self)}")

        if _ptype(self) == PAGE_TYPE_ROOT:
            self.set_page_type(PAGE_TYPE_INTERNAL)

        index = self.find_leaf_index_to_insert_page(_minkey(new_node))

        nslots = self.slots.copy()
        nslots.insert(index, _id(new_node))
        nkeys = self.keys.copy()

        if index > 0:
            nkeys.insert(index-1, _minkey(new_node))
        else:
            nkeys.insert(0, ref_minkey(nslots[0]))

        i = int(len(nkeys) / 2)

        self.keys = nkeys[:i]
        self.slots = nslots[:i+1]
        self.key_count = len(self.keys)
        self.set_min_key(ref_minkey(self.slots[0]))

        new_right_pg = global_palloc()
        new_right_pg.type = _ptype(self)
        
        new_right = bt_node(_ptype(self), self.level, new_right_pg)
        _info(f"set new_right node {_id(new_right)}={_ptype(self)}")

        new_right.keys = nkeys[i+1:]
        new_right.slots = nslots[i+1:]

        new_right.key_count = len(new_right.keys)
        new_right.page.min_key = _minkey(ref_page(new_right.slots[0]))

        new_right.update_header_buffer()
        self.update_header_buffer()

        return new_right
    
    def ref_slot(self, index):
        pg = ref_page(self.slots[index])
        _info(f"ref_slot: index={index}; page_id={self.slots[index]}")
        return bt_node.as_btnode(pg)
    
    def set_min_key(self, min_key):
        self.page.min_key = min_key
    
    def activate(self):
        pass

    @classmethod
    def create_xlog_btree_slot_insert(self, xid, target_page_id, new_page_id, slot_index):
        from core.wal import XLogBtreeInsertSlotCMD, XLogBtreeInsertSlotCMDPayload
        return XLogBtreeInsertSlotCMD(xid, XLogBtreeInsertSlotCMDPayload(target_page_id, new_page_id, slot_index))
    
    def direct_insert(self, inode, ctx=None):
        assert len(self.slots) < MAX_SLOT_COUNT
        assert len(self.slots) == len(self.keys) + 1

        if _ptype(inode) == PAGE_TYPE_HEAP:
            assert _ptype(self) == PAGE_TYPE_DATA

        index = self.find_leaf_index_to_insert_page(_minkey(inode))

        if _minkey(inode) < self.page.min_key:
            assert index == 0

        if ctx is not None:
            xlog = bt_node.create_xlog_btree_slot_insert(ctx.xid, _id(self), _id(inode), index)
            ctx.wal_writer.write_xlog(xlog)

        if index > 0:
            self.slots.insert(index, _id(inode))
            self.keys.insert(index-1, _minkey(inode))
        else:
            first_slot = self.slots[0]
            self.keys.insert(0, ref_minkey(first_slot))
            self.slots.insert(0, _id(inode))
            self.set_min_key(_minkey(inode))

        _info(f"direct insert: kindex={index}, keys={self.keys}, slots={self.slots}")

        self.key_count += 1
        assert len(self.keys) == self.key_count

        if _ptype(inode) == PAGE_TYPE_DATA:
            # prev node always exists
            old_prev_btn = self.ref_slot(index-1)

            if index != self.key_count:
                inode.set_next_page_id(_id(self.ref_slot(index + 1)))
            
            else:
                inode.set_next_page_id(old_prev_btn.next_page_id)

            old_prev_btn.set_next_page_id(_id(inode))
            old_prev_btn.update_header_buffer()
            inode.update_header_buffer()

        self.update_header_buffer()
        return index
    
    def get_internal_node_idx_to_go_down(self, tuple_key):
        for i, k in enumerate(self.keys):
            if tuple_key < k:
                return i
        return len(self.slots) - 1

    def get_internal_node_to_go_down(self, tuple_key):
        return self.slots[self.get_internal_node_idx_to_go_down(tuple_key)]

    def find_leaf_index_to_insert_page(self, tuple_key):
        idx = 0

        _info(f"direct insert: {tuple_key} to {_id(self)},")

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
        page_type = _ptype(self)
        if page_type == PAGE_TYPE_ROOT:
            return "btree root"
        elif page_type == PAGE_TYPE_INTERNAL:
            return "btree internal"
        elif page_type == PAGE_TYPE_DATA:
            return "btree data"
        else:
            raise Exception(f"unknwon btree page type: {page_type}")
    
    def update_header_buffer(self):
        #assert len(self.slots) > 0
        self.page.update_header_buffer()

        buffer = _buffer(self) 
        c = buffer_cursor(buffer, self.page.id)

        c.advance(HDR_SIZE)
        c.write_int64(self.level)
        c.write_int64(self.key_count)

        if len(self.slots) == 0:
            return c.pad(PAGE_SIZE - c.c)

        for k in self.keys:
            c.write_int64(k)

        c.pad( (MAX_KEY_COUNT - self.key_count) * 8 )

        for s in self.slots:
            c.write_int64(s)

        c.pad( (MAX_SLOT_COUNT - (self.key_count + 1)) * 8 )
        c.write_int64(self.next_page_id)

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
        next_page_id = cur.read_int64()

        if len(slots) == 1 and slots[0] == NULL_PAGE:
            slots = []

        return id, type, min_key, level, key_count, keys, slots, next_page_id

    def apply_header_buffer(self):
        id, type, min_key, level, key_count, keys, slots, next_page_id = self.parse_header_buffer(_buffer(self.page))

        self.page.id = id
        self.page.type = type
        self.set_min_key(min_key)

        self.level = level
        self.key_count = key_count
        self.keys = keys
        self.slots = slots
        self.next_page_id = next_page_id
    
    def search(self, key) -> tuple | None:
        target = self
        
        while not is_btree_data_page(target):
            vnode_id = target.get_internal_node_to_go_down(key)
            vnode = ref_page(vnode_id)
            
            vnode = bt_node.as_btnode(vnode)
            target = vnode

        found_heap_page = None 
        heap_page = target.get_internal_node_to_go_down(key)

        assert _ptype(target) == PAGE_TYPE_DATA
        assert _ptype(heap_page) == PAGE_TYPE_HEAP