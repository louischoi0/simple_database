from utils.buffer_cursor import buffer_cursor
from core.page_mgr import sys_hpalloc
from core.const import *

global SYS_OBJECTS
SYS_OBJECTS = {}
SYS_OBJECTS__NAME = {}

def register_sys_object(oid, object):
    SYS_OBJECTS[oid] = object
    SYS_OBJECTS__NAME[object.name] = object
  
def get_sys_object(oid):
    return SYS_OBJECTS[oid]

def get_sys_object_by_name(name):
    return SYS_OBJECTS__NAME[name]
  
def get_sys_object_id(name):
    return get_sys_object_by_name(name).oid
  
def get_sys_object_namespace(oid):
    return get_sys_object(oid).namespace

CATALOG_PAGE_ID__SYS_TABLE_TYPES_DESC = 0
CATALOG_PAGE_ID__SYS_TABLE_TABLES_TABLE_DESC = 1

def type_equal(type, type_name):
    return get_type(type_name).oid == type.oid

def get_type_oid(type_name):
    return get_type(type_name).oid

class Object:
    def __init__(self, oid, namespace, name, type, attrs=None, rel_id=None, value=None, value_type=None, value_is_null=True):
        self.oid = oid
        self.namespace = namespace
        self.type = type
        self.rel_id = rel_id

        self.name = name
        self.attrs = attrs if attrs is not None else []

        self.value = value
        self.value_type = value_type
        self.value_is_null = value_is_null
    
    def display(self):
        print(f"oid={self.oid}")
        print(f"namespace={self.namespace.name}")
        print(f"type={self.type.name}")
        print(f"rel_id={self.rel_id}")
        print(f"name={self.name}")
        print(f"value={self.value}")
  
    def ser(self) -> bytes:
        cursor = buffer_cursor()
        cursor.write_int64_a(self.oid)
        cursor.write_int64_a(self.namespace.oid)
        cursor.write_int64_a(self.type.oid)
        cursor.write_int64_a(self.rel_id)

        cursor.write_varchar_a(self.name)

        Attribute.write_attr_buffer(self.attrs, cursor) 

        if self.value_is_null:
            cursor.write_int64_a(PRIMITIVE_NULL_TYPE_FLAG)
            return cursor.buffer

        if type_equal(self.value_type, "varchar"):
            cursor.write_int64_a(PRIMITIVE_VARCHAR_TYPE_FLAG)
            cursor.write_varchar_a(self.value)
        elif type_equal(self.value_type, "int"):
            cursor.write_int64_a(PRIMITIVE_INT_TYPE_FLAG)
            cursor.write_int64_a(self.value)

        return cursor.buffer
    
    @classmethod
    def parse(cls, buffer: bytes):
        cursor = buffer_cursor(buffer)
        data = {}

        print("parse: ", type(buffer))

        data["oid"] = cursor.read_int64()
        namespace_oid = cursor.read_int64()
        data["namespace"] = get_sys_object(namespace_oid)

        type_oid = cursor.read_int64()
        data["type"] = get_sys_object(type_oid)

        data["rel_id"] = cursor.read_int64()
        data["name"] = cursor.read_varchar()

        data["attrs"] = Attribute.read_attr_buffer(cursor)
        data["value"], data["value_type"], data["value_is_null"] = Object.read_value(cursor)

        return Object(**data)
    
    @classmethod
    def read_value(self, cursor):
        value_type_flag = cursor.read_int64()

        if value_type_flag == PRIMITIVE_NULL_TYPE_FLAG:
            return None, None, True
        
        elif value_type_flag == PRIMITIVE_VARCHAR_TYPE_FLAG:
            data = cursor.read_varchar()
            return data, get_type("varchar"), False
        
        elif value_type_flag == PRIMITIVE_INT_TYPE_FLAG:
            data = cursor.read_int64()
            return data, get_type("int"), False

    def add_attr(self, obj):
        # circular attach prohibited
        assert self.type.oid != get_type("attribute").oid
        self.attrs.append(obj)

class SysObject(Object):
    def __init__(self, id, namespace, name, type, attrs=None, rel_id=0, value=None, value_type=None, value_is_null=True):
        attrs = attrs if attrs is not None else []
        super(SysObject, self).__init__(id, namespace, name, type, attrs, rel_id, value=value, value_is_null=value_is_null, value_type=value_type)
        register_sys_object(id, self)

class Attribute(Object):
    def __init__(self, oid, rel_id, name, value=None, value_type=None, value_is_null=True):
        super(Attribute, self).__init__(oid, obj_sys_namespace, name, get_type("attribute"), rel_id=rel_id, value=value, value_is_null=value_is_null, value_type=value_type)

    @classmethod
    def write_attr_buffer(cls, attrs, cursor):
        print(f"write attr num: {len(attrs)}")
        cursor.write_int64_a(len(attrs))
        for attr in attrs:
            cursor.write_bytes_a(attr.ser())

    @classmethod
    def read_attr_buffer(cls, cursor):
        attrs = []
        num = cursor.read_int64()
        for _ in range(num):
            buf = cursor.read_bytes()
            attr = Object.parse(buf)
            attrs.append(attr)
        return attrs

class SysType:
    def __init__(self, name, size):
        self.name = name
        self.size = size

    @classmethod
    def wrap(cls, obj):
        assert obj.type == get_type("type")
        return SysType(obj.name, obj.value)

# primatives
obj_sys_namespace = SysObject(0, None, "namespaceSys", None)
obj_sys_namespace.namespace = obj_sys_namespace

def get_sys_namespace():
    return obj_sys_namespace

type_type = Object(1, None, "typeType", None)
type_type.namespace = get_sys_namespace()
type_type.type = type_type

obj_sys_namespace.type = type_type

# sys types
type_page = SysObject(10, obj_sys_namespace, "typePage", type_type)
type_table = SysObject(11, obj_sys_namespace, "typeTable", type_type)
type_int = SysObject(12, obj_sys_namespace, "typeInt", type_type)
type_varchar = SysObject(13, obj_sys_namespace, "typeVarchar", type_type)
type_char = SysObject(13, obj_sys_namespace, "typeVarchar", type_type)
type_schema = SysObject(14, obj_sys_namespace, "typeSchema", type_type)
type_namespace = SysObject(15, obj_sys_namespace, "typeNamespace", type_type)
type_attrbute = SysObject(16, obj_sys_namespace, "typeAttribute", type_type)

TYPES = {
  "page": type_page,
  "table": type_table,
  "int": type_int,
  "varchar": type_varchar,
  "char": type_char,
  "attribute": type_attrbute,
}

def get_type(name): 
  return TYPES[name]

obj_sys_types_table = SysObject(100, obj_sys_namespace, "types", get_type("table"))
obj_sys_types_table.add_attr(Attribute(101, get_sys_object_id("types"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TYPES_DESC))

obj_sys_types_table = SysObject(110, obj_sys_namespace, "tables", get_type("table"))
obj_sys_types_table.add_attr(Attribute(111, get_sys_object_id("tables"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TABLES_TABLE_DESC))

# oid, name, fixed, version 

def bootstrap_catalog_type_heap():
    system_catalog_type_page = sys_hpalloc(CATALOG_PAGE_ID__SYS_TYPES_TABLE_DESC)
    system_catalog_type_page.initial_insert()




