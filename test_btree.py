from core.btree import bt_node
from core.const import *
from core.heap import HeapTuple, StructuredTuple
from core.catalog import Schema, Column, get_type
from utils.buffer_cursor import buffer_cursor
from app import kdapp

TEST_DRIVER = 2

simple_schema = Schema([
    Column(0, 0, "id", get_type("int"), notnull=True, default_val=None),
])

blk = None

def get_test_keys():
    keys = [ 1,2,3,4,5,6,7,8,9,10 ]

    for k in keys:
        yield k

def get_key(simple_tuple):
    cursor = buffer_cursor(simple_tuple.buffer)
    cursor.advance(StructuredTuple.HEAP_TUPLE_HEADER_SIZE)
    return cursor.read_int64()

def create_simple_tuple(id):
    t = StructuredTuple.load(simple_schema, { "id": id })
    t.min_key = id
    return t

def new_root_page(allocator, min_key):
    new_root_page = allocator.palloc()
    new_root_page.type = PAGE_TYPE_ROOT

    btn = bt_node(PAGE_TYPE_ROOT, 1, new_root_page)
    data_page = new_data_page_ini(allocator, min_key)
    btn.slots = [data_page.page.id]

    return btn

def new_data_page_ini(allocator, min_key):
    new_page = allocator.palloc()
    new_page.type = PAGE_TYPE_DATA
    new_page.min_key = min_key

    btn = bt_node(PAGE_TYPE_DATA, 0, new_page)

    nhpage = new_heap_page()

    nhpage.min_key = min_key
    nhpage.insert(HeapTuple(min_key))

    btn.slots = [nhpage.id]
    btn.keys = []

    btn.update_header_buffer()
    nhpage.update_header_buffer()

    app.blk.write_page(nhpage)
    app.blk.write_page(new_page)

    return btn

def insert_new_heap_page_to_root(blk, root_page, initial_tuple):
    btn = bt_node.as_btnode(root_page)

    h = app.alloc.hpalloc()
    h.insert(initial_tuple)
    h.update_header_buffer()

    btn.insert(h)
    btn.update_header_buffer()

    blk.write_page(h)
    blk.write_page(btn)
  
if __name__ == "__main__":
    app = kdapp(TEST_DRIVER)
    app.blk.init_driver()
    blk = app.blk

    key_gen = get_test_keys()

    btn = new_root_page(app.alloc, next(key_gen))
    blk.write_page(btn)

    insert_new_heap_page_to_root(blk, btn, create_simple_tuple(next(key_gen)))
