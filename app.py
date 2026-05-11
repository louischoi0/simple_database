import sys
import threading

from core.blk import _init_blk_driver
from core.page_mgr import page_allocator, _init_mgr_module
from core.page import get_page_name, is_btree_page, is_heap_page
from core.const import *
from core.btree import bt_node
from core.heap import heap_page, HeapTuple, StructuredTuple
from core.helper import _buffer, _id, _ptype, _minkey
from core.catalog import Schema, Column, get_type, get_type_val
from utils.logging import info, set_log_disable 
from core.dbmaster import DBMaster

app = None
_info = lambda x: info("app", x)

simple_schema = Schema([
    Column(0, 0, "id", get_type("int"), notnull=True, defval=None, type_val=get_type_val("int")),
])

blk = None

def get_test_keys():
    keys = [ 1,2,3,4,5,6,7,8,9,10 ]

    for k in keys:
        yield k

def get_simple_key(simple_tuple):
    cursor = buffer_cursor(simple_tuple.buffer)
    cursor.advance(StructuredTuple.HEAP_TUPLE_HEADER_SIZE)
    return cursor.read_int64()

key_gen = get_test_keys()

def create_simple_tuple(id):
    t = StructuredTuple.load(simple_schema, { "id": id })
    t.min_key = id
    return t

def new_heap_page():
    heap_page = app.alloc.hpalloc()
    return heap_page

def new_data_page_ini(allocator, min_key):
    new_page = allocator.palloc()
    new_page.type = PAGE_TYPE_DATA
    new_page.min_key = min_key

    btn = bt_node(PAGE_TYPE_DATA, 0, new_page)

    nhpage = new_heap_page()

    nhpage.min_key = min_key
    nhpage.insert(create_simple_tuple(next(key_gen)))

    btn.slots = [nhpage.id]
    btn.keys = []

    btn.update_header_buffer()
    nhpage.update_header_buffer()

    app.blk.write_page(nhpage)
    app.blk.write_page(new_page)

    return btn

def new_root_page(allocator, min_key):
    new_root_page = allocator.palloc()
    new_root_page.type = PAGE_TYPE_ROOT

    btn = bt_node(PAGE_TYPE_ROOT, 1, new_root_page)
    data_page = new_data_page_ini(allocator, min_key)
    btn.slots = [data_page.page.id]

    return btn

def check_heap_page(hpage):
    hpage.update_header_buffer()
    id, type, r_min_key, tuple_count, slot_cursor = hpage.parse_header_buffer(_buffer(hpage))

    assert hpage.id == id
    assert hpage.type == type
    assert hpage.min_key == r_min_key
    assert hpage.tuple_count == tuple_count
    assert hpage.slot_cursor == slot_cursor

    print("type: ", get_page_name(type))
    print("min_key: ", r_min_key)
    print("tuple_count: ", tuple_count)

def check_btree_page(bpage, print_flag=True):
    bpage.update_header_buffer()

    id, type, r_min_key, level, key_count, keys, slots, next_page_id = bt_node.parse_header_buffer(_buffer(bpage))
    assert bpage.page.type == type

    if print_flag:
        print("id: ", _id(bpage))
        print("type: ", get_page_name(type))
        print("min_key: ", _minkey(bpage))
        print("keys: ", bpage.keys)
        print("slots: ", bpage.slots)
        print("key_count: ", bpage.key_count)
        print("next_page_id: ", bpage.next_page_id)

    assert id == bpage.page.id
    assert type == bpage.page.type
    assert r_min_key == bpage.page.min_key
    assert bpage.slots == slots
    assert bpage.key_count == key_count
    assert bpage.next_page_id == next_page_id
    assert bpage.keys == sorted(bpage.keys)

def exec_command(cmd, app):
    ctype = cmd[0]
    _info(f"exec: {ctype}")

    blk = app.blk

    if ctype == "init":
        blk.init_driver()
    
    elif ctype == "new_root":
        min_key = int(cmd[1])
        root_page = new_root_page(app.alloc, min_key)

        root_page.update_header_buffer()
        check_btree_page(root_page, print_flag=False)

        blk.write_page(root_page.page)
        blk.commit_metablock(app.alloc.metablock)
    
    elif ctype == "insert_bt":
        root_page_id = int(cmd[1])
        new_key = int(cmd[2])

        page = blk.read_page(root_page_id)
        btn = bt_node.as_btnode(page)

        h = new_heap_page()
        h.insert(create_simple_tuple(new_key))
        h.mark_min_key(new_key)
        h.update_header_buffer()

        btn.insert(h)
        btn.update_header_buffer()

        blk.write_page(h)
        blk.write_page(btn)
    
    elif ctype == "read":
        set_log_disable()

        page_id = int(cmd[1])

        page = blk.read_page(page_id)

        if is_btree_page(page):
            btn = bt_node.as_btnode(page)
            check_btree_page(btn)
        
        elif is_heap_page(page):
            heap = page.as_heap()
            check_heap_page(heap)

        else:
            raise Exception(f"invalid page type: {page.type}")
    
    elif ctype == "test":
        h = new_heap_page()
        h.id = 9
        h.min_key = 11
        h.tuple_count = 2
        buffer = h.ser_header()
        a, b, c, d = heap_page.parse_header_buffer(buffer)

    elif ctype == "new_heap":
        h = new_heap_page()
        blk.write_page(h)

        blk.commit_metablock(app.alloc.metablock)

    elif ctype == "insert":
        page_id = int(cmd[1])
        value = int(cmd[2])

        page = blk.read_page(page_id)
        hpage = page.as_heap()
        hpage.apply_header_buffer()

        assert hpage.type == PAGE_TYPE_HEAP
        assert hpage.id == page_id

        hpage.insert(HeapTuple(value))

        blk.write_page(hpage)
        blk.commit_metablock(app.alloc.metablock)
    
    elif ctype == "iter":
        page_id = int(cmd[1])

        page = blk.read_page(page_id)

        assert page.type == PAGE_TYPE_HEAP
        assert page.id == page_id

        hpage = page.as_heap()
        hpage.apply_header_buffer()
        _info("iter page %d (%d k)" % (hpage.id, hpage.tuple_count))
        hpage.iter(print)
        blk.commit_metablock(app.alloc.metablock)
   
    else: 
        raise Exception("unknown command type: ", ctype)

PROCS = {}

def start_app_procs():
    for th in PROCS:
        th.start()

def bootstrap_main():
    app = DBMaster()
    app.activate()
    return app
    
if __name__ == "__main__":
    app = bootstrap_main()

    if sys.argv[1] == "test":
        exit(0)

    exec_command(sys.argv[1:], app)

    if sys.argv[1] != "init":
        app.blk.commit_metablock(app.alloc.metablock)
