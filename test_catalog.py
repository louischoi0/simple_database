from core.catalog import Object, SysObject, get_sys_namespace, get_type, Attribute, get_sys_object_id

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

def test_attr_dec():
    attr0 = Attribute(101, 99, "table_desc_page", value=123, value_is_null=False, value_type=get_type("int"))
    buffer = attr0.ser()
    attr2 = Object.parse(buffer)

    attr2.display()

if __name__ == '__main__':
    import pytest
    pytest.main()