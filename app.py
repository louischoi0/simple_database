import sys
import threading

from core.blk import _init_blk_driver
from core.page_mgr import page_allocator, _init_mgr_module, ref_heap_page
from core.page import get_page_name, is_btree_page, is_heap_page
from core.const import *
from core.btree import bt_node
from core.heap import heap_page, HeapTuple, StructuredTuple
from core.helper import _buffer, _id, _ptype, _minkey
from core.catalog import Schema, Column, get_type, get_type_val, get_public_namespace, init_table_access, raw_build_schema_from_sys_columns
from utils.logging import info, set_log_disable
from core.dbmaster import DBMaster

_info = lambda x: info("app", x)

simple_schema = Schema([
    Column(99, 88, 0, "id", get_type_val("int"), notnull=True, defval=None),
])

blk = None
ENABLE_WAL_SYSTEM = True

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
    from core.page_mgr import global_hpalloc
    heap_page = global_hpalloc()
    return heap_page

def new_data_page_ini(app, allocator, min_key):
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

def new_root_page(app, allocator, min_key):
    new_root_page = allocator.palloc()
    new_root_page.type = PAGE_TYPE_ROOT
    new_root_page.min_key = min_key

    btn = bt_node(PAGE_TYPE_ROOT, 1, new_root_page)
    data_page = new_data_page_ini(app, allocator, min_key)
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

def exec_command(cmd):
    ctype = cmd[0]

    if ctype == "init":
        blk = _init_blk_driver(0)
        blk.init_driver()

        from core.meta import _init_meta_system
        from core.meta import get_metablock
        _init_meta_system(blk)
        get_metablock().bootstrap()
        #app.cache_pool.autocommit()
        exit(0)
        return None

    elif ctype == "bootstrap":
        app = bootstrap_main(ENABLE_WAL_SYSTEM)
        from core.catalog import bootstrap_catalog_sys_types, bootstrap_catalog_sys_objects, bootstrap_catalog_sys_tables, bootstrap_catalog_sys_columns

        bootstrap_catalog_sys_types(app.alloc)
        bootstrap_catalog_sys_objects(app.alloc)
        bootstrap_catalog_sys_tables(app.alloc)

        bootstrap_catalog_sys_columns(app.alloc, "columns")
        bootstrap_catalog_sys_columns(app.alloc,"types")
        bootstrap_catalog_sys_columns(app.alloc,"objects")
        bootstrap_catalog_sys_columns(app.alloc,"tables")
        app.cache_pool.autocommit()
        exit(0)
    
    elif ctype == "new_root":
        app = bootstrap_main(ENABLE_WAL_SYSTEM)
        
        min_key = int(cmd[1])
        root_page = new_root_page(app, app.alloc, min_key)

        root_page.update_header_buffer()
        check_btree_page(root_page, print_flag=False)

        app.blk.write_page(root_page.page)
    
    elif ctype == "set_desc_pg_id":
        app = bootstrap_main(ENABLE_WAL_SYSTEM)
        table_oid = int(cmd[1])
        new_desc_pg_id = int(cmd[2])

        from core.catalog import raw_update_sys_tables_table_desc
        raw_update_sys_tables_table_desc(table_oid, new_desc_pg_id)
    
    elif ctype == "insert_bt":
        app = bootstrap_main(ENABLE_WAL_SYSTEM)
        root_page_id = int(cmd[1])
        new_key = int(cmd[2])
        table_oid = 4001

        page = app.blk.read_page(root_page_id)
        btn = bt_node.as_btnode(page)

        h = new_heap_page()
        d0 = { "student_id": new_key, "name": "louis", "grade": 3}
        schema = raw_build_schema_from_sys_columns(table_oid)

        t = StructuredTuple.load(schema, d0)

        h.insert(t)
        h.mark_min_key(new_key)
        h.update_header_buffer()

        btn.insert(h)
        btn.update_header_buffer()

        app.blk.write_page(h)
        app.blk.write_page(btn)
    
    elif ctype == "read":
        app = bootstrap_main(False)
        set_log_disable()

        page_id = int(cmd[1])
        page = app.blk.read_page(page_id)

        if is_btree_page(page):
            btn = bt_node.as_btnode(page)
            check_btree_page(btn)
        
        elif is_heap_page(page):
            heap = page.as_heap()
            check_heap_page(heap)

        else:
            raise Exception(f"invalid page type: {page.type}")
    
    elif ctype == "test":
        app = bootstrap_main(False) 
        h = new_heap_page()
        h.id = 9
        h.min_key = 11
        h.tuple_count = 2
        buffer = h.ser_header()
        a, b, c, d = heap_page.parse_header_buffer(buffer)
    
    elif ctype == "read_wal":
        set_log_disable()
        app = bootstrap_main(False) 
        app.wal_checkpointer.read_from(0, 0)

    elif ctype == "new_heap":
        app = bootstrap_main(False)
        h = new_heap_page()
        app.blk.write_page(h)
    
    elif ctype == "bt_insert_tp": 
        table_oid = int(cmd[1])
        new_key = int(cmd[2])
        app = bootstrap_main(ENABLE_WAL_SYSTEM)

        from core.executor import init_insert, QueryExecutionCtx
        d0 = { "student_id": new_key, "name": "louis", "grade": 3}
        ctx = QueryExecutionCtx(1, app.alloc, app.wal_writer)
        from core.catalog import get_sys_namespace

        insert_query_state = init_insert(get_public_namespace(), table_oid, d0)
        insert_query_state.exec(ctx)
    
    elif ctype == "select_heap":
        app = bootstrap_main(False)
        set_log_disable()
        table_oid = int(cmd[1])
        heap_page_id = int(cmd[2])
        table_access = init_table_access(get_public_namespace(), table_oid, lockmode=None)

        heap_page = ref_heap_page(heap_page_id)
        read_datas = heap_page.raw_map(lambda buffer: StructuredTuple.parse(buffer).struct(table_access.schema))

        for data in read_datas:
            print(data)

    elif ctype == "insert":
        app = bootstrap_main(ENABLE_WAL_SYSTEM)
        page_id = int(cmd[1])
        value = int(cmd[2])

        page = app.blk.read_page(page_id)
        hpage = page.as_heap()
        hpage.apply_header_buffer()

        assert hpage.type == PAGE_TYPE_HEAP
        assert hpage.id == page_id

        hpage.insert(HeapTuple(value))
        app.blk.write_page(hpage)
    
    elif ctype == "tables":
        set_log_disable()
        app = bootstrap_main(False)
        from core.catalog import read_sys_table
        tuples = read_sys_table("tables")
        for t in tuples:
            print(t)
    
    elif ctype == "create":
        app = bootstrap_main(False)

        test_table_schema = Schema([
            Column(999, 4001, 0, "student_id", get_type_val("int"), notnull=True, defval=None),
            Column(999, 4001, 1, "name", get_type_val("varchar"), notnull=True, defval=None),
            Column(999, 4001, 2, "grade", get_type_val("int"), notnull=True, defval=None),
        ])

        from core.catalog import create_table
        table_oid = create_table(app.alloc, get_public_namespace(), "students", schema=test_table_schema, clustered_type="btree")
        print(table_oid)
        app.cache_pool.autocommit()
    
    elif ctype == "iter":
        app = bootstrap_main(False)
        page_id = int(cmd[1])

        page = app.blk.read_page(page_id)

        assert page.type == PAGE_TYPE_HEAP
        assert page.id == page_id

        hpage = page.as_heap()
        hpage.apply_header_buffer()
        _info("iter page %d (%d k)" % (hpage.id, hpage.tuple_count))
        hpage.iter(print)
   
    else: 
        raise Exception("unknown command type: ", ctype)
    
    return app

def bootstrap_main(background=True):
    import os
    num = os.environ.get("DRIVE_NUM", 0)
    app = DBMaster(driver_num=int(num))

    if not background:
        app.disable_background_proc()

    app.activate()
    return app
    
if __name__ == "__main__":
    if sys.argv[1] == "test":
        exit(0)

    app = exec_command(sys.argv[1:])

    if sys.argv[1] != "init":
        app.meta.commit_metablock()

        if not ENABLE_WAL_SYSTEM:
            app.cache_pool.autocommit()
