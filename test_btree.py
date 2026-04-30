from core.btree import bt_node
from core.const import *
from core.heap import HeapTuple, StructuredTuple
from core.catalog import Schema, Column, get_type
from utils.buffer_cursor import buffer_cursor
from app import kdapp

app = kdapp()

simple_schema = Schema([
    Column(0, 0, "id", get_type("int"), notnull=True, default_val=None),
])

def get_key(simple_tuple):
    cursor = buffer_cursor(simple_tuple.buffer)
    cursor.advance(StructuredTuple.HEAP_TUPLE_HEADER_SIZE)
    return cursor.read_int64()

def create_simple_tuple(id):
    return StructuredTuple.load(simple_schema, { "id": id })

def new_root_page(allocator, min_key):
    new_root_page = allocator.palloc()
    new_root_page.type = PAGE_TYPE_ROOT

    btn = bt_node(PAGE_TYPE_ROOT, 1, new_root_page)
    data_page = new_data_page_ini(allocator, create_simple_tuple(min_key))

    btn.slots = [data_page.page.id]

    return btn

def new_data_page_ini(allocator, min_key):
    new_page = allocator.palloc()
    new_page.type = PAGE_TYPE_DATA
    new_page.min_key = min_key

    btn = bt_node(PAGE_TYPE_DATA, 0, new_page)

    nhpage = app.alloc.hpalloc()

    nhpage.min_key = min_key
    nhpage.insert(create_simple_tuple(min_key))

    btn.slots = [nhpage.id]
    btn.keys = []

    btn.update_header_buffer()
    nhpage.update_header_buffer()

    app.blk.write_page(nhpage)
    app.blk.write_page(new_page)

    return btn
  
if __name__ == "__main__":
    btn = new_root_page(app.alloc, 1)