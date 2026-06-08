from core.catalog import Column, get_type, init_table_access
from core.heap import StructuredTuple, heap_page as HeapPage, HeapTuple
from core.page_mgr import ref_heap_page, ref_btree_page, global_hpalloc, page_allocator, ref_page
from core.catalog import get_table_schema_from_cache, is_table_clustered_heap, is_table_clustered_btree, raw_build_schema_from_sys_columns
from core.catalog import raw_update_sys_tables_table_desc, sys_int64_index_schema, TableAccess
from core.wal import XLogWriter
from core.heap import insert_with_grow
from core.helper import _ptype, _id
from core.const import *
from core.btree import bt_node, bt_cursor
from core.tx import TransactionManager
from enum import Enum

from dataclasses import dataclass
from utils.logging import info

_info = lambda *x: info("executor", *x)
IndexValueCol = sys_int64_index_schema.col_map["value"]

NO_ERR = 0
BTREE_ALLOC_HEAP_FACTOR = 0.05


class ExecutionError:
    NO_HEAP_TUPLE = ERR_NO_HEAP_TUPLE


    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

class QueryOperator:
    def __init__(self, name, *args):
        self.name = name
        self.args = args

class Equal(QueryOperator):
    def __init__(self, lhs, rhs):
        super(Equal, self).__init__("equal", lhs, rhs)
        self.lhs = lhs 
        self.rhs = rhs

    def __call__(self, heap_tuple):
        return unwrap(heap_tuple, self.lhs) == unwrap(heap_tuple, self.rhs)

class Range(QueryOperator):
    def __init__(self, start, end, value):
        super(Range, self).__init__("range", start, end, value)
        self.start = start
        self.end = end
        self.value = value

    def __call__(self):
        return unwrap(self.start) <= unwrap(self.value) and unwrap(self.value) < unwrap(self.end)

def unwrap(heap_tuple, value):
    if isinstance(value, Column):
        return heap_tuple.data[value.pos]
    elif isinstance(value, QueryOperator):
        return value(heap_tuple)
    return value

class QueryExecState:
    def __init__(self, table_access, *args, **kwargs):
        self.table_access = table_access
        self.result = None
        self.error = None
    
    def on_finished(self):
        if self.table_access.obj_lock is not None:
            self.table_access.obj_lock.release()
        
    def set_result(self, res):
        self.result = res
        return NO_ERR
    
    def set_error(self, error):
        self.error = error
        return error.code

@dataclass
class QueryExecutionCtx:
    xid: int
    allocator: page_allocator
    wal_writer: XLogWriter
    tx_mgr: TransactionManager

class QueryOperation(Enum):
    UPDATE = "update"
    DELETE = "delete"

class UndoLog:
    def __init__(self, operation: QueryOperation, owner_oid, old_pk, old_tuple_buffer):
        self.old_pk = old_pk
        self.owner_oid = owner_oid
        self.old_tuple_buffer = old_tuple_buffer      
        self.operation = operation
    
class HeapPageUpdateState(QueryExecState):
    def __init__(self, table_access: TableAccess, heap_page_id, pk, new_tuple):
        super(HeapPageUpdateState, self).__init__(table_access)
        self.pk = pk
        self.new_tuple = new_tuple
        self.heap_page_id = heap_page_id
    
    def exec(self, ctx: QueryExecutionCtx):
        _heap_page = ctx.allocator.ref_heap_page(self.heap_page_id)
        old_tuple_buffer = _heap_page.search(self.pk)

        if old_tuple_buffer is None:
            return self.set_error(code=ExecutionError.NO_HEAP_TUPLE, msg="tuple not found in heap page")

        HeapTuple.write_xmax(old_tuple_buffer, ctx.xid)

        undo = UndoLog(QueryOperation.UPDATE, self.table_access.oid, self.pk, old_tuple_buffer)
        ctx.tx_mgr.append_undo_log(undo, undo.owner_oid, ctx.old_pk)

        t = StructuredTuple.load(self.table_access.schema, self.new_tuple)
        t.struct(self.table_access.schema)

        _heap_page.update(ctx=ctx, pk=self.pk, new_tuple=t)
        return self.set_result(1)

class HeapPageInsertState(QueryExecState):
    def __init__(self, table_access, tuple):
        super(HeapPageInsertState, self).__init__(table_access)
        self.tuple = tuple

    def exec(self, ctx: QueryExecutionCtx):
        assert is_table_clustered_heap(self.table_access)

        heap_page = ref_heap_page(self.table_access.desc_pg_id)
        insert_with_grow(global_hpalloc, heap_page, self.tuple)
        return 1

class BtreePageExpandState(QueryExecState):
    def __init__(self, table_access, root_page_id, new_heap_page_min_key):
        super(BtreePageExpandState, self).__init__(table_access)
        self.root_page_id = root_page_id
        self.new_heap_page_min_key = new_heap_page_min_key
    
    def exec(self, ctx: QueryExecutionCtx):
        root_page = ref_btree_page(self.root_page_id)
        btn = bt_node.as_btnode(root_page)

        h = ctx.allocator.hpalloc()
        h.mark_min_key(self.new_heap_page_min_key)

        new_root = btn.insert(h, ctx=ctx)

        if _id(new_root) != _id(root_page):
            raw_update_sys_tables_table_desc(ctx, self.table_access.oid, _id(new_root))

        return 1

class BtreePageInsertTupleState(QueryExecState):
    def __init__(self, table_access, tuple):
        super(BtreePageInsertTupleState, self).__init__(table_access)
        self.tuple = tuple

    def exec(self, ctx: QueryExecutionCtx):
        assert is_table_clustered_btree(self.table_access)
        cursor = bt_cursor()

        btree_root_page = ref_btree_page(self.table_access.desc_pg_id)
        _info(f"execute page insert tuple on page {_id(btree_root_page)}")

        if btree_root_page.search(self.tuple.pk) is not None:
            raise Exception(f"duplicated key error: {self.tuple.pk}")

        if btree_root_page.empty():
            btree_root_page.insert_tuple_with_init(ctx.allocator, self.tuple, ctx=ctx)
            return self.set_result(self.tuple.pk)

        target_page = btree_root_page
        cursor.visit(target_page)

        while _ptype(target_page) != PAGE_TYPE_DATA:
            target_page_index = target_page.get_internal_node_idx_to_go_down(self.tuple.pk)
            target_page = ref_btree_page(target_page.slots[target_page_index])

        _info(f"tuple_id={self.tuple.pk}, target_page_id={_id(target_page)}, target_page_index={target_page_index}, key_count={target_page.key_count}")

        heap_page_index = target_page.find_leaf_index_to_insert_page(self.tuple.pk)
        heap_page: HeapPage = ref_heap_page(target_page.slots[heap_page_index-1])
        _heap_page_id = _id(heap_page)
        new_root = None

        if heap_page_index == 0:
            new_heap_page = global_hpalloc()
            new_heap_page.insert(self.tuple, ctx=ctx)
            new_heap_page.mark_min_key(self.tuple.pk)

            new_root = btree_root_page.insert(new_heap_page, ctx)

        elif (heap_page_index == target_page.key_count + 1 and (heap_page.usage_pct() > BTREE_ALLOC_HEAP_FACTOR or not heap_page.possible(self.tuple.size))) :
            new_heap_page = heap_page.split_insert(self.tuple, ctx)
            assert _heap_page_id == _id(heap_page)
            _info(f"heap page has been splitted from {_id(heap_page)} to {_id(new_heap_page)}")
            new_root = btree_root_page.insert(new_heap_page, ctx)

        else:
            _info(f"insert tuple #{self.tuple.pk} to exisiting heap page #{heap_page.id}")

            if heap_page.possible(self.tuple.size):
                ret = heap_page.insert(self.tuple)
                assert ret >= 0
            else:
                new_heap_page = heap_page.split_insert(self.tuple, ctx)
                new_root = btree_root_page.insert(new_heap_page, ctx)

        if new_root is not None and _id(new_root) != _id(btree_root_page):
            raw_update_sys_tables_table_desc(ctx, self.table_access.oid, _id(new_root))
            
        return self.set_result(self.tuple.pk)
    
class BtreePageGetTupleState(QueryExecState):
    def __init__(self, table_access, pk):
        super(BtreePageGetTupleState, self).__init__(table_access)
        self.pk = pk
    
    def exec(self, ctx: QueryExecutionCtx):
        assert is_table_clustered_btree(self.table_access)

        _info(f"btree page get tuple #{self.pk}: ", self.table_access.desc_pg_id)

        btree_root_page: bt_node = ref_btree_page(self.table_access.desc_pg_id)
        _res = btree_root_page.search(self.pk)

        if _res is None:
            _info("tuple #{self.pk} is not found")
        else:
            _info(f"tuple #{self.pk} found: {StructuredTuple.parse(_res, self.table_access.schema).struct(self.table_access.schema)}")
            self.set_result( StructuredTuple.parse(_res, self.table_access.schema).struct(self.table_access.schema) )

        return NO_ERR

class BuildIndexState(QueryExecState):
    def __init__(self, table_access, target_col):
        super(BuildIndexState, self).__init__(table_access)
        self.target_col = target_col
    
    def exec(self, ctx: QueryExecutionCtx=None):
        mem_sorted_set = {}
        index_tuple_id = 0

        def get_index_column_value(heap_tuple):
            return heap_tuple.get(self.target_col)

        def build_func(heap_tuple: StructuredTuple):
            nonlocal index_tuple_id

            heap_tuple.struct(self.table_access.schema)
            value = get_index_column_value(heap_tuple)
            index_tuple_id += 1

            index_tuple = StructuredTuple.load(sys_int64_index_schema, { "id": index_tuple_id, "value": value, "key": heap_tuple.pk })

            if value not in mem_sorted_set:
                mem_sorted_set[value] = [ index_tuple ]
            else:
                mem_sorted_set[value].append(index_tuple)

        scan_execution = BtreePageScanState(self.table_access, func=build_func)
        scan_execution.exec(ctx)
        heap_pages = []

        for value_key in mem_sorted_set:
            heap = ctx.allocator.hpalloc()
            heap_pages.append(heap)
            heap.mark_min_key(value_key)

            for t in mem_sorted_set[value_key]:
                insert_with_grow(ctx.allocator, heap, t, ctx)

        btree_root = bt_node.new_root_page(ctx.allocator)
        first_heap_page = heap_pages[0]

        new_data_page = ctx.allocator.palloc()
        new_data_page.type = PAGE_TYPE_DATA
        new_data_page.min_key = first_heap_page.min_key

        btn = bt_node(PAGE_TYPE_DATA, 0, new_data_page)
        btn.slots = [ _id(first_heap_page) ]
        btn.keys = []
        btn.update_header_buffer()

        btree_root.slots = [ _id(btn) ]
        btree_root.keys = []
        btree_root.key_count = 0
        btree_root.min_key = first_heap_page.min_key

        for idx, heap_page in enumerate(heap_pages[1:]):
            btree_root = btree_root.insert(heap_page)

class BtreeIndexPageScanState(QueryExecState):
    def __init__(self, table_access, index_entry_pg_id, predicate):
        super(BtreeIndexPageScanState, self).__init__(table_access)
        self.predicate = predicate
        self.index_entry_pg_id = index_entry_pg_id

    def exec(self, ctx: QueryExecutionCtx):
        access = TableAccess(None, None, sys_int64_index_schema, self.index_entry_pg_id, "btree", 1)
        index_scan_execution = BtreePageScanState(table_access=access, predicate=self.predicate)
        index_scan_execution.exec(ctx)
        res = []

        for t in index_scan_execution.result:
            _exec = BtreePageGetTupleState(self.table_access, t.get("key"))
            _exec.exec(ctx)

            if _exec.result is not None:
                res.append(_exec.result)
        
        self.set_result(res)
        return NO_ERR

class BtreePageScanState(QueryExecState):
    def __init__(self, table_access, func=None, predicate=None):
        super(BtreePageScanState, self).__init__(table_access)
        self.func = func
        self.predicate = predicate
    
    def exec(self, ctx: QueryExecutionCtx):
        root_page = ref_btree_page(self.table_access.desc_pg_id)
        res = []

        if root_page.empty():
            self.set_result([])
            return True
        
        def scan_heap_page(heap_page):
            data = heap_page.raw_map(lambda buffer: StructuredTuple.parse(buffer, schema=self.table_access.schema), ctx)

            if self.predicate is not None:
                data = filter(self.predicate, data)

            if self.func is not None:
                data = map(self.func, data)

            return data

        def visit_node(parent, node_page_id):
            assert _ptype(parent) != PAGE_TYPE_HEAP

            node = ref_page(node_page_id)
            _info(f"scan btree page: {_id(node)}")

            if _ptype(node) == PAGE_TYPE_HEAP:
                node = node.as_heap()
                node.activate()
                _info(f"collect heap page: {_id(node)}")
                return res.extend( scan_heap_page(node) )

            node = bt_node.as_btnode(node)

            for slot in node.slots:
                visit_node(node, slot)

        for slot in root_page.slots:
            visit_node(root_page, slot)

        self.set_result(res)

        return 0

class HeapPageScanState(QueryExecState):
    def __init__(self, table_access, predicate=None):
        super(HeapPageScanState, self).__init__(table_access)
        self.current_ref_page = None
        self.current_slot_index = 0
        self.ref_pages = []
        self.results = []

        self.predicate = predicate

    def exec(self):
        heap_page = ref_heap_page(self.table_access.desc_pg_id)
        res = []

        while True:
            _res = heap_page.raw_filter(
                f=lambda buffer: StructuredTuple.parse(buffer).struct(self.table_access.schema),
                raw_filter_func=self.predicate
            )

            res.extend(_res)

            if heap_page.has_next():
                heap_page = ref_heap_page(heap_page.read_next_page_pointer())
            else:
                break

        return res
    
    def add_condition(self, condition):
        self.conditions.append(condition)
        return self

def init_execute_ddl(namespace, table_oid):
    table_access = init_table_access(namespace, table_oid, lockmode=2)
    schema = get_table_schema_from_cache(table_oid)

    if schema is None:
        schema = raw_build_schema_from_sys_columns(table_oid)
    
    return table_access

def init_insert(namespace, table_oid, raw_data):
    table_access = init_table_access(namespace, table_oid, lockmode=2)
    schema = get_table_schema_from_cache(table_oid)

    if schema is None:
        schema = raw_build_schema_from_sys_columns(table_oid)

    data_tuple = StructuredTuple.load(schema, raw_data)
    _info(f"init insert: table_oid={table_oid}; table_desc_id={table_access.desc_pg_id}; data={data_tuple.struct(schema)};")

    if table_access.clustered_type == "heap":
        return HeapPageInsertState(table_access=table_access, tuple=data_tuple)
    elif table_access.clustered_type == "btree":
        return BtreePageInsertTupleState(table_access=table_access, tuple=data_tuple)

def init_select(namespace, table_oid):
    table_access = init_table_access(namespace, table_oid, lockmode=1)
    #todo match scanstate type acording to table clustered type

    if table_access.clustered_type == "heap":
        return HeapPageScanState(table_access=table_access)
    elif table_access.clustered_type == "btree":
        return BtreePageScanState(table_access=table_access)

def init_update(namespace, table_oid):
    table_access = init_table_access(namespace, table_oid, lockmode=1)