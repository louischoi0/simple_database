from core.heap import StructuredTuple, heap_page as HeapPage
from core.catalog import Column, get_type, Schema, bootstrap_catalog_sys_columns, sys_types_schema
from core.page import page
from core.blk import _init_blk_driver

test_table_schema = Schema([
    Column(0, 0, "student_id", get_type("int"), notnull=True, default_val=None),
    Column(0, 1, "name", get_type("varchar"), notnull=True, default_val=None),
    Column(0, 2, "grade", get_type("int"), notnull=True, default_val=None),
])

data_template = {
    "student_id": 0,
    "name": "student_",
    "grade": 1,
}

blk_driver = _init_blk_driver(1) 
blk_driver.init_driver()

def test_structed_tuple2():
    st = StructuredTuple.load(test_table_schema, data_template)
    st2 = StructuredTuple.parse(st.buffer)

    st2.struct(test_table_schema)
    st.struct(test_table_schema)

    assert st2.size == st.size

    for k in data_template:
        assert st.get(k) == st2.get(k)

def test_structured_tuple():
    datas = []

    for i in range(10):
        item = data_template.copy()
        item["student_id"] = i
        item["name"] += str(i)
        item["grade"] = i % 4

        item = StructuredTuple.load(test_table_schema, item)
        item.struct(test_table_schema)

        datas.append(item)

    heap = HeapPage(0)

    for i in datas:
        heap.insert(i)

    heap.delete_tuple_by_index(0) 
    read_datas = heap.raw_map(lambda buffer: StructuredTuple.parse(buffer).struct(test_table_schema))

    datas = datas[1:]
    
    for a, b in zip(datas, read_datas):
        for k in data_template:
            assert a.get(k) == b[k]
        
    assert heap.tuple_count == len(datas) + len(heap.deleted)
    assert len(datas) == len(read_datas)

    heap.update_header_buffer()
    blk_driver.write_page(heap)

    read_heap_page = blk_driver.read_page(heap.id)
    read_heap_page = read_heap_page.as_heap()
    read_heap_page.activate()

    assert read_heap_page.tuple_count == heap.tuple_count
    assert heap.checksum() == read_heap_page.checksum()
    print(heap.checksum())

if __name__ == '__main__':
    test_structed_tuple2()
    test_structured_tuple()

