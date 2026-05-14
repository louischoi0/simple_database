from core.catalog import Object, SysObject, get_sys_namespace, get_type, Attribute, get_sys_object_id, sys_columns_schema
from core.catalog import bootstrap_catalog_sys_types, read_sys_types_tuples, bootstrap_catalog_sys_columns, read_sys_table, read_sys_columns_tuples
from core.catalog import bootstrap_catalog_sys_objects, bootstrap_catalog_sys_tables, Column, get_type_val, Schema, create_table
from core.catalog import sys_types_schema
from core.heap import StructuredTuple, heap_page as HeapPage
from core.blk import _init_blk_driver
from core.dbmaster import DBMaster

def test_object_dec():
    obj = SysObject(1, get_sys_namespace(), "testObject", get_type("varchar"), value="thisisvalue", value_is_null=False, value_type=get_type("varchar"))

    print("obj attr: ", obj.attrs)
    buffer = obj.ser()
    obj2 = Object.parse(buffer)

    assert obj.oid == obj2.oid
    assert obj.namespace == obj2.namespace
    assert obj.rel_id == obj2.rel_id
    assert obj.type.oid == obj2.type.oid
    assert obj.name == obj2.name

    obj2.display()

def test_structured_tuple():
    data = {
        "oid": 1,
        "name": "type_int",
        "type_val": 0,
        "len": 8
    }

    structured_tuple = StructuredTuple.load(sys_types_schema, data)
    structured_tuple2 = StructuredTuple.parse(structured_tuple.buffer)

    structured_tuple2.struct(sys_types_schema)
    structured_tuple.struct(sys_types_schema)

    assert structured_tuple.structured_data["oid"] == structured_tuple2.structured_data["oid"]
    assert structured_tuple.structured_data["name"] == structured_tuple2.structured_data["name"]
    assert structured_tuple.structured_data["type_val"] == structured_tuple2.structured_data["type_val"]
    assert structured_tuple.structured_data["len"] == structured_tuple2.structured_data["len"]
    assert len(structured_tuple.structured_data) == len(structured_tuple2.structured_data)

def test_create_table(app):

    test_table_schema = Schema([
        Column(0, 4001, 0, "student_id", get_type_val("int"), notnull=True, defval=None),
        Column(0, 4001, 1, "name", get_type_val("varchar"), notnull=True, defval=None),
        Column(0, 4001, 2, "grade", get_type_val("int"), notnull=True, defval=None),
    ])

    create_table(app.alloc, get_sys_namespace(), "students", schema=test_table_schema, clustered_type="btree")

if __name__ == '__main__':
    app = DBMaster(2)

    app.activate()
    app.blk.init_driver()

    test_object_dec()
    test_structured_tuple()

    bootstrap_catalog_sys_types()
    tuples = read_sys_table("types")
    for t in tuples:
        print(t)

    bootstrap_catalog_sys_objects()
    tuples = read_sys_table("objects")
    for t in tuples:
        print(t)

    tuples = bootstrap_catalog_sys_tables()
    tuples = read_sys_table("tables")
    for t in tuples:
        print(t)

    bootstrap_catalog_sys_columns("columns")
    bootstrap_catalog_sys_columns("types")
    bootstrap_catalog_sys_columns("objects")
    bootstrap_catalog_sys_columns("tables")

    tuples = read_sys_table("columns")
    for t in tuples:
        print(t)

    test_create_table(app)

    tuples = read_sys_table("tables")
    for t in tuples:
        print(t)

    app.terminate()