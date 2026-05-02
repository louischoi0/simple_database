from core.catalog import Object, SysObject, get_sys_namespace, get_type, Attribute, get_sys_object_id
from core.catalog import bootstrap_catalog_sys_types, read_sys_types_tuples, bootstrap_catalog_sys_columns, read_sys_table, read_sys_columns_tuples
from core.catalog import sys_types_schema
from core.heap import StructuredTuple, heap_page as HeapPage
from core.blk import _init_blk_driver
from core.dbmaster import DBMaster


def __test_object_dec():
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

def __test_attr_dec():
    attr0 = Attribute(101, 99, "table_desc_page", value=123, value_is_null=False, value_type=get_type("int"))
    buffer = attr0.ser()
    attr2 = Object.parse(buffer)

    attr2.display()


def test_structured_tuple():
    data = {
        "oid": 1,
        "name": "type_int",
        "type_val": 0,
        "len": 8
    }

    structured_tuple = StructuredTuple.load(sys_types_schema, data)
    structured_tuple2 = StructuredTuple.parse(sys_types_schema, structured_tuple.buffer)

    assert structured_tuple.structured_data["oid"] == structured_tuple2.structured_data["oid"]
    assert structured_tuple.structured_data["name"] == structured_tuple2.structured_data["name"]
    assert structured_tuple.structured_data["type_val"] == structured_tuple2.structured_data["type_val"]
    assert structured_tuple.structured_data["len"] == structured_tuple2.structured_data["len"]
    assert len(structured_tuple.structured_data) == len(structured_tuple2.structured_data)


if __name__ == '__main__':
    app = DBMaster(2)
    app.activate()

    bootstrap_catalog_sys_types(app.blk)
    tuples = read_sys_table(app.blk, "types")

    bootstrap_catalog_sys_columns(app.blk)
    tuples = read_sys_table(app.blk, "columns")
    print(tuples)
