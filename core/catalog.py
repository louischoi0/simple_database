from utils.buffer_cursor import buffer_cursor
from core.page_mgr import sys_hpalloc_ref, ref_page, global_hpalloc, ref_heap_page
from core.const import *
from core.heap import StructuredTuple, insert_with_grow
from utils.logging import info

_info = lambda x: info("catalog", x)

global SYS_OBJECTS

global OBJECT_CACHE 
OBJECT_CACHE = {
}

SYS_OBJECTS = {}
SYS_OBJECTS__NAME = {}

TABLE_SCHEMA_CACHE = {}

def cache_table_schema(oid, schema):
    assert isinstance(schema, Schema)
    TABLE_SCHEMA_CACHE[oid] = schema

def get_table_schema_from_cache(oid):
    try:
        return TABLE_SCHEMA_CACHE[oid]
    except KeyError:
        return None

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


def type_equal(type, type_name):
    return get_type(type_name).oid == type.oid

def get_type_oid(type_name):
    return get_type(type_name).oid

def get_type_name(type_name):
    return get_type(type_name).name

def get_type_val(type_name):
    return get_type(type_name).value

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
        cursor.write_dynamic_type_a(self.value_type.value, self.value)

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


obj_sys_namespace = SysObject(0, None, "namespaceSys", None, value=0, value_is_null=False)
obj_public_namespace = SysObject(1, None, "namespacePublic", None, value=1, value_is_null=False)

def get_sys_namespace():
    return obj_sys_namespace

def is_sys_namespace(namespace):
    return obj_sys_namespace.oid == namespace.oid

def get_public_namespace():
    return obj_public_namespace

type_type = Object(2, None, "typeType", None)
type_type.namespace = get_sys_namespace()
type_type.type = type_type

# sys types

type_int = SysObject(12, obj_sys_namespace, "int", type_type, value=0, value_is_null=False, value_type=0)
type_int.value_type = type_int
type_varchar = SysObject(13, obj_sys_namespace, "type_varchar", type_type, value=1, value_is_null=False, value_type=type_int)
type_char = SysObject(13, obj_sys_namespace, "type_char", type_type, value=2, value_is_null=False, value_type=type_int)
type_bool = SysObject(15, obj_sys_namespace, "type_bool", type_type, value=3, value_is_null=False, value_type=type_int)
type_bytes = SysObject(16, obj_sys_namespace, "type_bytes", type_type, value=4, value_is_null=False, value_type=type_int)

type_schema = SysObject(14, obj_sys_namespace, "type_schema", type_type, value=5, value_is_null=False, value_type=type_int)
type_namespace = SysObject(17, obj_sys_namespace, "type_namespace", type_type, value=6, value_is_null=False, value_type=type_int)
type_attrbute = SysObject(18, obj_sys_namespace, "type_attribute", type_type, value=7, value_is_null=False, value_type=type_int)
type_column = SysObject(19, obj_sys_namespace, "type_column", type_type, value=8, value_is_null=False, value_type=type_int)

type_page = SysObject(20, obj_sys_namespace, "type_page", type_type, value=9, value_is_null=False, value_type=type_int)
type_table = SysObject(21, obj_sys_namespace, "type_table", type_type, value=10, value_is_null=False, value_type=type_int)
type_operator = SysObject(22, obj_sys_namespace, "type_operator", type_type, value=11, value_is_null=False, value_type=type_int)

obj_sys_namespace.type = type_namespace
obj_sys_namespace.value_type = type_int

obj_public_namespace.type = type_namespace
obj_public_namespace.value_type = type_int

TYPES = {
  "type_page": type_page,
  "type_table": type_table,
  "type_namespace": type_namespace,

  "type_column": type_column,
  "type_operator": type_operator,

  "type_int": type_int,
  "type_varchar": type_varchar,
  "type_char": type_char,
  "type_bool": type_bool,
  "type_attribute": type_attrbute,
  "type_bytes": type_bytes,
}

TYPES_LEN = {
  "type_page": 8,
  "type_table": 8,
  "type_namespace": 8,

  "type_int": 8,
  "type_varchar": 0,
  "type_char": 0,
  "type_bool": 1,
  "type_bytes": 0,
}

def get_type_by_val(type_val):
    for t in TYPES:
        t = TYPES[t]
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

def get_type_len_by_val(val):
    return get_type_len(get_type_by_val(val).name)

def add_attr(ref_oid, attr):
    pass

# primatives
obj_sys_namespace.namespace = obj_sys_namespace

obj_sys_types_table = SysObject(100, obj_sys_namespace, "types", get_type("table"))
obj_sys_classes_table = SysObject(110, obj_sys_namespace, "objects", get_type("table"))
obj_sys_columns_table = SysObject(111, obj_sys_namespace, "columns", get_type("table"))
obj_sys_tables_table = SysObject(112, obj_sys_namespace, "tables", get_type("table"))
obj_sys_attributes_table = SysObject(113, obj_sys_namespace, "attributes", get_type("table"))
obj_sys_proc_table = SysObject(114, obj_sys_namespace, "proc", get_type("table"))

#add_attr(get_sys_object_id("types"), SysAttribute(101, get_sys_object_id("types"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TYPES_DESC))
#add_attr(get_sys_object_id("classes"), SysAttribute(111, get_sys_object_id("tables"), "table_desc_page", CATALOG_PAGE_ID__SYS_TABLE_TABLES_TABLE_DESC))

CATALOG__SYS_TYPES_TABLE_DESC = 4
CATALOG__SYS_COLUMNS_TABLE_DESC = 5
CATALOG__SYS_OBJECTS_TABLE_DESC = 6
CATALOG__SYS_TABLES_TABLE_DESC = 7

SYS_TABLE_DESC_MAP = {
    "types": CATALOG__SYS_TYPES_TABLE_DESC,
    "columns": CATALOG__SYS_COLUMNS_TABLE_DESC,
    "objects": CATALOG__SYS_OBJECTS_TABLE_DESC,
    "tables": CATALOG__SYS_TABLES_TABLE_DESC,
}

def get_sys_table_desc(name: str) -> int:
    return SYS_TABLE_DESC_MAP[name]

class Column(Object):
    def __init__(self, oid, rel_id, pos, name, type_val, notnull=True, defval=None, len=None):
        self.oid = oid
        self.rel_id = rel_id
        self.pos = pos
        self.name = name
        self.type_val = type_val
        self.type = get_type_by_val(type_val)
        self.len = get_type_len_by_val(type_val) if len is None else len
        self.notnull = notnull
        self.defval = defval

class Schema:
    def __init__(self, columns):
        self.col_arr = columns
        self.col_map = { x.name: x for x in columns }
    
    def has_defval(self, index):
        return self.col_arr[index].defval is not None
    
    def get_defval(self, index):
        return self.col_arr[index].defval
    
    def is_notnull(self, index):
        return self.col_arr[index].notnull
    
    def get(self, name):
        return self.col_map[name]
    
    def get_attribute(self, name, attr):
        return getattr(self.col_map[name], attr)

    def get_type_val(self, name):
        col = self.col_map[name]
        return col.type_val
    
    def raw(self):
        res = []
        for col in self.col_arr:
            d = {}
            d["oid"] = col.oid
            d["rel_id"] = col.rel_id
            d["pos"] = col.pos
            d["name"] = col.name
            d["type"] = col.type
            d["type_val"] = col.type_val
            d["len"] = col.len
            d["notnull"] = col.notnull
            d["defval"] = col.defval

            res.append(d)
        return res

global SYS_COL_OID_COUNTER
SYS_COL_OID_COUNTER = 1000

def generate_sys_col_oid():
    global SYS_COL_OID_COUNTER
    SYS_COL_OID_COUNTER += 1
    return SYS_COL_OID_COUNTER

global USER_OBJ_OID_COUNTER
USER_OBJ_OID_COUNTER = 4000

def generate_user_oid():
    global USER_OBJ_OID_COUNTER
    USER_OBJ_OID_COUNTER += 1
    return USER_OBJ_OID_COUNTER 

sys_columns_schema = Schema([
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 0, "oid", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 1, "rel_id", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 2, "pos", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 3, "name", get_type_val("varchar"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 4, "type_val", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 5, "len", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 6, "notnull", get_type_val("bool"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("columns"), 7, "defval", get_type_val("bytes"), notnull=True, defval=None),
])

cache_table_schema(get_sys_object_id("columns"), sys_columns_schema)

sys_types_schema = Schema([
    Column(generate_sys_col_oid(), get_sys_object_id("types"), 0, "oid", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("types"), 1, "name", get_type_val("varchar"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("types"), 2, "type_val", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("types"), 3, "len", get_type_val("int"), notnull=True, defval=None),
])

cache_table_schema(get_sys_object_id("types"), sys_types_schema)

sys_objects_schema = Schema([
    Column(generate_sys_col_oid(), get_sys_object_id("objects"), 0, "oid", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("objects"), 1, "namespace", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("objects"), 2, "obj_type", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("objects"), 3, "name", get_type_val("varchar"), notnull=True, defval=None),
])

cache_table_schema(get_sys_object_id("objects"), sys_objects_schema)

sys_tables_schema = Schema([
    Column(generate_sys_col_oid(), get_sys_object_id("tables"), 0, "oid", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("tables"), 1, "namespace", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("tables"), 2, "name", get_type_val("varchar"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("tables"), 3, "desc_page_id", get_type_val("int"), notnull=True, defval=None),
    Column(generate_sys_col_oid(), get_sys_object_id("tables"), 4, "clustered_type", get_type_val("varchar"), notnull=True, defval=None),
])

cache_table_schema(get_sys_object_id("tables"), sys_tables_schema)

def bootstrap_catalog_sys_objects():
    objects_raw_tuples = [
        {
            "oid": get_sys_object_id("types"),  
            "namespace": get_sys_namespace().value, 
            "obj_type": get_type_val("table"),
            "name": get_sys_object_by_name("types").name, 
        },
        {
            "oid": get_sys_object_id("objects"),  
            "namespace": get_sys_namespace().value, 
            "obj_type": get_type_val("table"),
            "name": get_sys_object_by_name("objects").name, 
        },
        {
            "oid": get_sys_object_id("columns"),  
            "namespace": get_sys_namespace().value, 
            "obj_type": get_type_val("table"),
            "name": get_sys_object_by_name("columns").name, 
        },
        {
            "oid": get_sys_object_id("tables"),  
            "namespace": get_sys_namespace().value, 
            "obj_type": get_type_val("table"),
            "name": get_sys_object_by_name("tables").name, 
        },
    ]

    hpage = sys_hpalloc_ref(get_sys_table_desc("objects"))

    for d in objects_raw_tuples:
        objects_tuple = StructuredTuple.load(sys_objects_schema, d)
        objects_tuple.struct(sys_objects_schema)
        hpage = insert_catalog_sys_object(hpage, objects_tuple)

    _info(f"objects row initialized: {hpage.checksum()}")

def bootstrap_catalog_sys_tables():
    tables_raw_tuples = [
        {
            "oid": get_sys_object_id("types"),  
            "namespace": get_sys_namespace().value, 
            "name": get_sys_object_by_name("types").name, 
            "desc_page_id": get_sys_table_desc("types"),
            "clustered_type": "heap"
        },
        {
            "oid": get_sys_object_id("objects"),  
            "namespace": get_sys_namespace().value, 
            "name": get_sys_object_by_name("objects").name, 
            "desc_page_id": get_sys_table_desc("objects"),
            "clustered_type": "heap"
        },
        {
            "oid": get_sys_object_id("columns"),  
            "namespace": get_sys_namespace().value, 
            "name": get_sys_object_by_name("columns").name, 
            "desc_page_id": get_sys_table_desc("columns"),
            "clustered_type": "heap"
        },
        {
            "oid": get_sys_object_id("tables"),  
            "namespace": get_sys_namespace().value, 
            "name": get_sys_object_by_name("tables").name, 
            "desc_page_id": get_sys_table_desc("tables"),
            "clustered_type": "heap"
        },
    ]

    hpage = sys_hpalloc_ref(get_sys_table_desc("tables"))

    for d in tables_raw_tuples:
        objects_tuple = StructuredTuple.load(sys_tables_schema, d)
        hpage = insert_catalog_sys_table(hpage, objects_tuple)

    _info(f"tablesrow initialized: {hpage.checksum()}")

def insert_catalog_sys_table(heap_page, table_tuple):
    table_tuple.struct(sys_tables_schema)
    return insert_with_grow(global_hpalloc, heap_page, table_tuple)

def insert_catalog_sys_object(heap_page, objects_tuple):
    objects_tuple.struct(sys_objects_schema)
    return insert_with_grow(global_hpalloc, heap_page, objects_tuple)

def insert_catalog_sys_columns(heap_page, schema: Schema):
    tuples = schema.raw()

    for column_tuple in tuples:
        t = StructuredTuple.load(sys_columns_schema, column_tuple)
        heap_page = insert_with_grow(global_hpalloc, heap_page, t)

def create_table(namespace, name, schema, clustered_type="heap"):
    object_hpage = sys_hpalloc_ref(get_sys_table_desc("objects"))
    column_hpage = sys_hpalloc_ref(get_sys_table_desc("columns"))
    table_hpage = sys_hpalloc_ref(get_sys_table_desc("tables"))

    new_table_oid = generate_user_oid()

    table_hp = global_hpalloc()
    table_hp.update_header_buffer()

    object = {
        "oid": new_table_oid,
        "namespace": namespace.value,
        "obj_type": get_type_val("table"),
        "name": name,
    }

    object_tuple = StructuredTuple.load(sys_objects_schema, object)
    object_tuple.struct(sys_objects_schema)
    insert_catalog_sys_object(object_hpage, object_tuple)

    table = {
        "oid": new_table_oid,
        "namespace": namespace.value, 
        "name": name,
        "desc_page_id": table_hp.id,
        "clustered_type": clustered_type,
    }
    table_tuple = StructuredTuple.load(sys_tables_schema, table)
    table_tuple.struct(sys_tables_schema)
    insert_catalog_sys_table(table_hpage, table_tuple)

    insert_catalog_sys_columns(column_hpage, schema)

def bootstrap_catalog_sys_columns(sys_obj):
    _info(f"{sys_obj} bootstrapping...")
    hpage = sys_hpalloc_ref(get_sys_table_desc("columns"))
    oid = get_sys_object_id(sys_obj)
    schema = get_table_schema_from_cache(oid)

    if schema is None:
        raise Exception(f"schema for sys table {oid} must be always cached {sys_obj}")

    insert_catalog_sys_columns(hpage, schema)
    _info(f"{sys_obj} table initialized: {hpage.checksum()}")

def bootstrap_catalog_sys_types():
    _info(f"sys_types bootstrapping ...")
    hpage = sys_hpalloc_ref(get_sys_table_desc("types"))
    tuples = [
        {
            "oid": get_type_oid("int"),
            "name": get_type_name("int"),
            "type_val": get_type_val("int"),
            "len": get_type_len("int"),
        },
        {
            "oid": get_type_oid("varchar"),
            "name": get_type_name("varchar"),
            "type_val": get_type_val("varchar"),
            "len": get_type_len("varchar"),
        },
        {
            "oid": get_type_oid("char"),
            "name": get_type_name("char"),
            "type_val": get_type_val("char"),
            "len": get_type_len("char"),
        },
        {
            "oid": get_type_oid("bool"),
            "name": get_type_name("bool"),
            "type_val": get_type_val("bool"),
            "len": get_type_len("bool"),
        }
    ]

    for column_tuple in tuples:
        t = StructuredTuple.load(sys_types_schema, column_tuple)
        hpage = insert_with_grow(global_hpalloc, hpage, t)

    _info(f"sys_types table initialized: {hpage.checksum()}")

def read_sys_table(table_name):
    table_oid = get_sys_object_id(table_name)
    schema = get_table_schema_from_cache(table_oid)
    if schema is None:
        raise Exception(f"schema for sys table '{table_name}' must be always cached {table_name}")
    desc_page_id = get_sys_table_desc(table_name)

    page = ref_page(desc_page_id)
    heap_page = page.as_heap()
    heap_page.activate()

    return heap_page.raw_map(lambda buffer: StructuredTuple.parse(buffer).struct(schema))

def read_sys_types_tuples():
    page = ref_page(get_sys_table_desc("types"))
    heap_page = page.as_heap()
    heap_page.activate()

    return heap_page.raw_map(lambda buffer: StructuredTuple.parse(buffer).struct(sys_types_schema))

def read_sys_columns_tuples():
    page = ref_page(get_sys_table_desc("columns"))
    heap_page = page.as_heap()
    heap_page.activate()

    types = heap_page.raw_map(lambda buffer: StructuredTuple.parse(buffer).struct(sys_columns_schema))
    return types

class TableAccess:
    def __init__(self, namespace, oid, schema, desc_pg_id, clustered_type, lockmode=None):
        self.namespace = namespace
        self.oid = oid
        self.schema = schema
        self.desc_pg_id = desc_pg_id
        self.clustered_type = clustered_type

def raw_get_sys_tables(oid):
    page_heap = ref_heap_page(get_sys_table_desc("tables"))
    buffer = page_heap.raw_get(oid)

    if buffer is None:
        raise Exception(f"table row for oid:{oid} does not exists in sys.tables")
    
    return StructuredTuple.parse(buffer).struct(sys_tables_schema)

def raw_build_schema_from_sys_columns(oid):
    schema = get_table_schema_from_cache(oid)
    if schema is not None:
        return schema

    table_desc_id = get_sys_table_desc("columns")
    page_heap = ref_heap_page(table_desc_id)

    columns = page_heap.raw_filter(
        f=lambda buffer: StructuredTuple.parse(buffer).struct(sys_columns_schema),
        raw_filter_func=lambda x: x["rel_id"] == oid
    )

    schema = Schema([ Column(**x) for x in columns ])
    cache_table_schema(oid, schema)
    return schema

def init_table_access(namespace, oid, lockmode=None):

    if is_sys_namespace(namespace):
        schema = get_table_schema_from_cache(oid)
        assert schema is not None
        desc = get_sys_table_desc()

    else: 
        table_row = raw_get_sys_tables(oid)
        desc = table_row["desc_page_id"]
        clustered_type = table_row["clustered_type"]
        schema = raw_build_schema_from_sys_columns(oid)

    return TableAccess(namespace, oid, schema, desc_pg_id=desc, clustered_type=clustered_type, lockmode=lockmode)

def is_table_clustered_heap(table_access):
    return table_access["clustered_type"] == "heap"

def is_table_clustered_btree(table_access):
    return table_access["clustered_type"] == "btree"
