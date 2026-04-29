from utils.buffer_cursor import buffer_cursor
from core.page_mgr import sys_hpalloc
from core.const import *

global SYS_OBJECTS

SYS_OBJECTS = {}
SYS_OBJECTS__NAME = {}
SYS_TABLE_SCHEMA_CACHE = {}

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

class SysObject(Object):
    def __init__(self, id, namespace, name, type, attrs=None, rel_id=0, value=None, value_type=None, value_is_null=True):
        attrs = attrs if attrs is not None else []
        super(SysObject, self).__init__(id, namespace, name, type, attrs, rel_id, value=value, value_is_null=value_is_null, value_type=value_type)
        register_sys_object(id, self)

class SysAttribute(Attribute):
    def __init__(self, oid, rel_id, name, value=None, value_type=None, value_is_null=True):
        super(SysAttribute, self).__init__(oid, obj_sys_namespace, name, get_type("attribute"), rel_id=rel_id, value=value, value_is_null=value_is_null, value_type=value_type)

class Attribute(Object):
    def __init__(self, oid, namespace, rel_id, name, value=None, value_type=None, value_is_null=True):
        super(Attribute, self).__init__(oid, namespace, name, get_type("attribute"), rel_id=rel_id, value=value, value_is_null=value_is_null, value_type=value_type)

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
type_bool = SysObject(15, obj_sys_namespace, "typeBool", type_type)
type_bytes = SysObject(16, obj_sys_namespace, "typeBytes", type_type)
type_namespace = SysObject(17, obj_sys_namespace, "typeNamespace", type_type)
type_attrbute = SysObject(18, obj_sys_namespace, "typeAttribute", type_type)
type_column = SysObject(19, obj_sys_namespace, "typeColumn", type_type)

TYPES = {
  "page": type_page,
  "table": type_table,
  "int": type_int,
  "varchar": type_varchar,
  "char": type_char,
  "bool": type_bool,
  "attribute": type_attrbute,
  "bytes": type_bytes,
}

TYPES_VALUE = {
  "int": 0,
  "varchar": 1,
  "char": 2,
  "bool": 3
}

def get_type(name): 
  return TYPES[name]

def get_type_value(name):
    return TYPES_VALUE[name]

def add_attr(ref_oid, attr):
    pass

obj_sys_types_table = SysObject(100, obj_sys_namespace, "types", get_type("table"))
obj_sys_classes_table = SysObject(110, obj_sys_namespace, "classes", get_type("table"))
obj_sys_columns_table = SysObject(110, obj_sys_namespace, "columns", get_type("table"))
obj_sys_attributes_table = SysObject(110, obj_sys_namespace, "attributes", get_type("table"))
obj_sys_proc_table = SysObject(110, obj_sys_namespace, "proc", get_type("table"))

add_attr(get_sys_object_id("types"), SysAttribute(101, get_sys_object_id("types"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TYPES_DESC))
add_attr(get_sys_object_id("classes"), SysAttribute(111, get_sys_object_id("tables"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TABLES_TABLE_DESC))


class Column(Object):
    def __init__(self, rel_id, pos, name, type_val, len, notnull=True, default=None):
        self.rel_id = rel_id
        self.pos = pos
        self.name = name
        self.type_val = type_val
        self.len = len
        self.notnull = notnull

sys_columns_schema = [
    Column(get_sys_object_id("columns"), 0, "rel_id", get_type_value("int"), notnull=True, default=None),
    Column(get_sys_object_id("columns"), 1, "pos", get_type_value("int"), notnull=True, default=None),
    Column(get_sys_object_id("columns"), 2, "name", get_type_value("varchar"), notnull=True, default=None),
    Column(get_sys_object_id("columns"), 3, "type_val", get_type_value("int"), notnull=True, default=None),
    Column(get_sys_object_id("columns"), 4, "len", get_type_value("int"), notnull=True, default=None),
    Column(get_sys_object_id("columns"), 5, "notnull", get_type_value("bool"), notnull=True, default=True),
    Column(get_sys_object_id("columns"), 5, "defval", get_type_value("bytes"), notnull=True, default=True),
]



# oid, name, fixed, version 
def bootstrap_catalog_type_heap():
    system_catalog_type_page = sys_hpalloc(CATALOG_PAGE_ID__SYS_TYPES_TABLE_DESC)
    system_catalog_type_page.initial_insert()




