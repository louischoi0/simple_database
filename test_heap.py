from core.heap import StructuredTuple, heap_page as HeapPage, insert_with_grow
from core.catalog import Column, get_type_val, get_type, Schema, bootstrap_catalog_sys_columns, sys_types_schema
from core.executor import QueryExecutionCtx
from core.page import page
from core.page_mgr import global_hpalloc
from core.blk import _init_blk_driver
from utils.buffer_cursor import buffer_cursor
from core.const import *
from core.dbmaster import DBMaster
from core.meta import get_metablock
from utils.logging import timer

test_table_schema = Schema([
    Column(0, 0, 0, "student_id", get_type_val("int"), notnull=True, defval=None),
    Column(0, 0, 1, "name", get_type_val("varchar"), notnull=True, defval=None),
    Column(0, 0, 2, "grade", get_type_val("int"), notnull=True, defval=None),
    #Column(0, 3, "grade2", get_type_val("int"), notnull=True, defval=None),
])

data_template = {
    "student_id": 0,
    "name": "student_",
    "grade": 1,
    #"grade2": 2,
}

def test_structed_tuple2():
    st = StructuredTuple.load(test_table_schema, data_template)
    st2 = StructuredTuple.parse(st.buffer)

    st2.struct(test_table_schema)
    st.struct(test_table_schema)

    assert st2.size == st.size
    assert st2.xmin == st.xmin
    assert st2.xmax == st.xmax

    for k in data_template:
        assert st.get(k) == st2.get(k)

def test_heap_page_rollback(app):
    data = {
        "student_id": 9,
        "name": "louis",
        "grade": 5
    }

    item = StructuredTuple.load(test_table_schema, data)
    item.struct(test_table_schema)

    heap = app.alloc.hpalloc()

    heap.insert(item)
    heap.rollback_insert(0)

    assert len(heap.slots) == 0
    assert heap.tuple_count == 0

@timer
def test_xmin_with_ctx(app):
    datas = []
    TEST_TXID = 72
    ctx = QueryExecutionCtx(TEST_TXID, app.alloc, app.wal_writer)

    for i in range(10):
        item = data_template.copy()
        item["student_id"] = i
        item["name"] += str(i)
        item["grade"] = i % 4

        item = StructuredTuple.load(test_table_schema, item)
        item.struct(test_table_schema)

        datas.append(item)

    heap = app.alloc.hpalloc()
    c = 0
    for i in datas:
        cursor = buffer_cursor(i.buffer)
        cursor.at(0)
        size = cursor.read_int64()
        heap.insert(i, ctx=ctx)

        c += 1
    
    from core.page_mgr import ref_heap_page
    read = ref_heap_page(heap.id)
    read = read.as_heap()
    read.activate()

    read_datas = heap.raw_map(lambda buffer: StructuredTuple.parse(buffer))
    for i in read_datas:
        assert i.xmin == TEST_TXID

def test_heap_page_grow(app):
    datas = []
    ctx = QueryExecutionCtx(72, app.alloc, app.wal_writer)

    for i in range(400):
        item = data_template.copy()
        item["student_id"] = i
        item["name"] += str(i)
        item["grade"] = i % 4

        item = StructuredTuple.load(test_table_schema, item)
        item.struct(test_table_schema)

        datas.append(item)

    heap = app.alloc.hpalloc()
    c = 0
    for i in datas:
        cursor = buffer_cursor(i.buffer)
        cursor.at(0)
        size = cursor.read_int64()

        heap = insert_with_grow(global_hpalloc, heap, i)
        c += 1
    
    next_page_id = heap.read_next_page_pointer()
    page = heap

    while next_page_id != NULL_PAGE:
        next_page_id = page.read_next_page_pointer()
        if next_page_id == 0:
            break
        
        else:
            assert page.id + 1 == next_page_id

        from core.page_mgr import ref_page
        page = ref_page(next_page_id)

def test_structured_tuple(app):
    datas = []

    for i in range(20):
        item = data_template.copy()
        item["student_id"] = i
        item["name"] += str(i)
        item["grade"] = i % 4

        item = StructuredTuple.load(test_table_schema, item)
        item.struct(test_table_schema)

        datas.append(item)

    heap = app.alloc.hpalloc()

    for i in datas:
        cursor = buffer_cursor(i.buffer)
        cursor.at(0)
        size = cursor.read_int64()

        #heap.insert(i)
        insert_with_grow(global_hpalloc, heap, i)

    heap.delete_tuple_by_index(0) 
    read_datas = heap.raw_map(lambda buffer: StructuredTuple.parse(buffer).struct(test_table_schema))

    datas = datas[1:]
    
    for a, b in zip(datas, read_datas):
        for k in data_template:
            assert a.get(k) == b[k]
        
        read_tuple = StructuredTuple.load(test_table_schema, b)
        #print(read_tuple.xmin)
        
    assert heap.tuple_count == len(datas) + len(heap.deleted)
    assert len(datas) == len(read_datas)

    heap.update_header_buffer()
    app.blk.write_page(heap)

    read_heap_page = app.blk.read_page(heap.id)
    read_heap_page = read_heap_page.as_heap()
    read_heap_page.activate()

    assert read_heap_page.tuple_count == heap.tuple_count
    assert heap.checksum() == read_heap_page.checksum()
    print(heap.checksum())

if __name__ == '__main__':
    app = DBMaster()
    app.disable_background_proc()
    app.activate()

    #test_heap_page_rollback(app)
    #test_structed_tuple2()
    test_structured_tuple(app)
    #test_heap_page_grow(app)
    #test_xmin_with_ctx(app)

    app.cache_pool.autocommit()
    app.meta.commit_metablock()
    app.terminate()