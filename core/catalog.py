from utils.buffer_cursor import buffer_cursor
from core.page_mgr import sys_hpalloc
from core.const import *

global SYS_OBJECTS

SYS_OBJECTS = {}
SYS_OBJECTS__NAME = {}

TABLE_SCHEMA_CACHE = {}

def cache_table_schema(oid, schema):
    TABLE_SCHEMA_CACHE[oid] = schema

def get_table_schema_from_cache(oid):
    return TABLE_SCHEMA_CACHE[oid]

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
        
        cursor.write_int64_a(PRIMITIVE_VARCHAR_TYPE_FLAG)
        cursor.write_dynamic_type_a(self.value_type)

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

class SysAttribute(Attribute):
    def __init__(self, oid, rel_id, name, value=None, value_type=None, value_is_null=True):
        super(SysAttribute, self).__init__(oid, obj_sys_namespace, name, get_type("attribute"), rel_id=rel_id, value=value, value_is_null=value_is_null, value_type=value_type)

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
type_page = SysObject(10, obj_sys_namespace, "type_page", type_type)
type_table = SysObject(11, obj_sys_namespace, "type_table", type_type)

type_int = SysObject(12, obj_sys_namespace, "int", type_type, value=0, value_is_null=False, value_type=0)
type_int.value_type = type_int
type_varchar = SysObject(13, obj_sys_namespace, "type_varchar", type_type, value=1, value_is_null=False, value_type=type_int)
type_char = SysObject(13, obj_sys_namespace, "type_char", type_type, value=2, value_is_null=False, value_type=type_int)
type_bool = SysObject(15, obj_sys_namespace, "type_bool", type_type, value=3, value_is_null=False, value_type=type_int)
type_bytes = SysObject(16, obj_sys_namespace, "type_bytes", type_type, value=4, value_is_null=False, value_type=type_int)

type_schema = SysObject(14, obj_sys_namespace, "type_schema", type_type)
type_namespace = SysObject(17, obj_sys_namespace, "type_namespace", type_type)
type_attrbute = SysObject(18, obj_sys_namespace, "type_attribute", type_type)
type_column = SysObject(19, obj_sys_namespace, "type_column", type_type)

TYPES = {
  "type_page": type_page,
  "type_table": type_table,
  "type_int": type_int,
  "type_varchar": type_varchar,
  "type_char": type_char,
  "type_bool": type_bool,
  "type_attribute": type_attrbute,
  "type_bytes": type_bytes,
}

TYPES_LEN = {
  "type_int": 8,
  "type_varchar": 0,
  "type_char": 0,
  "type_bool": 1,
  "type_bytes": 0,
}

def get_type_by_val(type_val):
    for t in TYPES:
        if t.value == type_val:
            return t

    raise Exception(f"no type for value: {type_val}")

def get_type(name): 
    if not name.startswith("type_"):
        name = "type_" + name
    return TYPES[name]

def get_type_value(name):
    if not name.startswith("type_"):
        name = "type_" + name
    return TYPES[name].value

def get_type_len(name):
    if not name.startswith("type_"):
        name = "type_" + name
    return TYPES_LEN[name]

def add_attr(ref_oid, attr):
    pass

obj_sys_types_table = SysObject(100, obj_sys_namespace, "types", get_type("table"))
obj_sys_classes_table = SysObject(110, obj_sys_namespace, "classes", get_type("table"))
obj_sys_columns_table = SysObject(110, obj_sys_namespace, "columns", get_type("table"))
obj_sys_attributes_table = SysObject(110, obj_sys_namespace, "attributes", get_type("table"))
obj_sys_proc_table = SysObject(110, obj_sys_namespace, "proc", get_type("table"))

#add_attr(get_sys_object_id("types"), SysAttribute(101, get_sys_object_id("types"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TYPES_DESC))
#add_attr(get_sys_object_id("classes"), SysAttribute(111, get_sys_object_id("tables"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TABLES_TABLE_DESC))

class Column(Object):
    def __init__(self, rel_id, pos, name, type, notnull=True, default_val=None):
        self.rel_id = rel_id
        self.pos = pos
        self.name = name
        self.type = type
        self.len = get_type_len(type.name)
        self.notnull = notnull
        self.default_val = default_val

sys_columns_schema = [
    Column(get_sys_object_id("columns"), 0, "rel_id", get_type("int"), notnull=True, default_val=None),
    Column(get_sys_object_id("columns"), 1, "pos", get_type("int"), notnull=True, default_val=None),
    Column(get_sys_object_id("columns"), 2, "name", get_type("varchar"), notnull=True, default_val=None),
    Column(get_sys_object_id("columns"), 3, "type_val", get_type("int"), notnull=True, default_val=None),
    Column(get_sys_object_id("columns"), 4, "len", get_type("int"), notnull=True, default_val=None),
    Column(get_sys_object_id("columns"), 5, "notnull", get_type("bool"), notnull=True, default_val=True),
    Column(get_sys_object_id("columns"), 5, "defval", get_type("bytes"), notnull=True, default_val=True),
]

cache_table_schema(get_sys_object_id("columns"), sys_columns_schema)

sys_types_schema = [
    Column(get_sys_object_id("types"), 0, "oid", get_type("int"), notnull=True, default_val=None),
    Column(get_sys_object_id("types"), 1, "name", get_type("varchar"), notnull=True, default_val=None),
    Column(get_sys_object_id("types"), 2, "type_val", get_type("int"), notnull=True, default_val=None),
    Column(get_sys_object_id("types"), 3, "len", get_type("int"), notnull=True, default_val=None),
]

cache_table_schema(get_sys_object_id("types"), sys_types_schema)

# oid, name, fixed, version 
def bootstrap_catalog_type_heap():
    system_catalog_type_page = sys_hpalloc(CATALOG_PAGE_ID__SYS_TABLE_TYPES_DESC)
    system_catalog_type_page.initial_insert()
