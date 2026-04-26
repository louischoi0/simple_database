import sys
import threading

from core.blk import _init_blk_driver
from core.page_mgr import page_allocator, _init_mgr_module, pg_mgr
from core.page import get_page_name, is_btree_page, is_heap_page
from core.const import *
from core.btree import bt_node
from core.heap import heap_page, heap_tuple
from core.helper import _buffer, _id, _ptype

app = None

def new_heap_page():
    heap_page = app.alloc.hpalloc()
    return heap_page

def new_bt_data(key):
    npage = app.alloc.palloc()
    bt_node = bt_node(key, PAGE_TYPE_DATA, 0, npage)

    bt_node.keys = []
    bt_node.key_count = 0
    bt_node.slots = [key]

    return bt_node

def new_data_page_ini(allocator, min_key):
    new_page = allocator.palloc()
    new_page.type = PAGE_TYPE_DATA
    new_page.min_key = min_key

    btn = bt_node(PAGE_TYPE_DATA, 0, new_page)

    nhpage = new_heap_page()

    nhpage.min_key = min_key
    nhpage.insert(heap_tuple(min_key))

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

    id, type, r_min_key, tuple_count = hpage.parse_header_buffer(_buffer(hpage))

    assert hpage.id == id
    assert hpage.type == type
    assert hpage.min_key == r_min_key
    assert hpage.tuple_count == tuple_count

    print("type: ", get_page_name(type))
    print("min_key: ", r_min_key)
    print("tuple_count: ", tuple_count)

def check_btree_page(bpage):
    bpage.update_header_buffer()

    id, type, r_min_key, level, key_count, keys, slots = bt_node.parse_header_buffer(_buffer(bpage))
    assert bpage.page.type == type

    print("type: ", get_page_name(type))
    print("keys: ", bpage.keys)
    print("slots: ", bpage.slots)
    print("key_count: ", bpage.key_count)

    assert id == bpage.page.id
    assert type == bpage.page.type
    assert r_min_key == bpage.page.min_key
    assert bpage.slots == slots
    assert bpage.key_count == key_count

def exec_command(cmd, app):
    ctype = cmd[0]
    print("exec: ", ctype)

    blk = app.blk

    if ctype == "init":
        blk.init_driver()
    
    elif ctype == "new_root":
        min_key = int(cmd[1])
        root_page = new_root_page(app.alloc, min_key)

        print(f"root page: {root_page.page.id}; slots: {root_page.slots}")

        root_page.update_header_buffer()
        check_btree_page(root_page)

        blk.write_page(root_page.page)
        blk.commit_metablock(app.alloc.metablock)
    
    elif ctype == "insert_bt":
        root_page_id = int(cmd[1])
        new_key = int(cmd[2])

        page = blk.read_page(root_page_id)
        btn = bt_node.as_btnode(page)

        h = new_heap_page()
        h.initial_insert(heap_tuple(new_key))
        h.update_header_buffer()

        btn.insert(h)

        btn.update_header_buffer()

        blk.write_page(h)
        blk.write_page(btn)
    
    elif ctype == "read":
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
        print(a,b,c,d)      

    elif ctype == "new_heap":
        h = new_heap_page()
        blk.write_page(h)
        print("heap page allocated: ", h.id, h.type)

        blk.commit_metablock(app.alloc.metablock)

    elif ctype == "insert":
        page_id = int(cmd[1])
        value = int(cmd[2])

        page = blk.read_page(page_id)
        hpage = page.as_heap()
        hpage.apply_header_buffer()

        assert hpage.type == PAGE_TYPE_HEAP
        assert hpage.id == page_id

        hpage.insert(heap_tuple(value))

        blk.write_page(hpage)
        blk.commit_metablock(app.alloc.metablock)
    
    elif ctype == "iter":
        page_id = int(cmd[1])

        page = blk.read_page(page_id)

        assert page.type == PAGE_TYPE_HEAP
        assert page.id == page_id

        hpage = page.as_heap()
        hpage.apply_header_buffer()
        print("iter page %d (%d k)" % (hpage.id, hpage.tuple_count))
        hpage.iter(print)
        blk.commit_metablock(app.alloc.metablock)
    
    else: 
        raise Exception("unknown command type: ", ctype)

class kdapp:
    def __init__(self):
        blk = _init_blk_driver(0)
        alloc, cache_pool = _init_mgr_module(blk)

        self.blk = blk
        self.alloc = alloc   
        self.cache_pool = cache_pool

PROCS = {}

def fork_pg_mgr_proc(blk, allocator):
    pg_mgr_inst = pg_mgr(blk, allocator.cache_pool)
    import threading
    
    th = threading.Thread(target=pg_mgr_inst.proc)

    PROCS["pg_mgr"] = th
    return th

def start_app_procs():
    for th in PROCS:
        th.start()

def bootstrap_main():
    app = kdapp()
    fork_pg_mgr_proc(app.blk, app.alloc)
    return app
    
if __name__ == "__main__":
    app = bootstrap_main()

    if sys.argv[1] == "test":
        exit(0)

    exec_command(sys.argv[1:], app)
    app.cache_pool.commit_all_pages()

    if sys.argv[1] != "init":
        app.blk.commit_metablock(app.alloc.metablock)
