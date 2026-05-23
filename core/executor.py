from core.catalog import Column, get_type, init_table_access
from core.heap import StructuredTuple
from core.page_mgr import ref_heap_page, ref_btree_page, global_hpalloc, page_allocator, ref_page
from core.catalog import get_table_schema_from_cache, is_table_clustered_heap, is_table_clustered_btree, raw_build_schema_from_sys_columns
from core.catalog import raw_update_sys_tables_table_desc
from core.wal import XLogWriter
from core.heap import insert_with_grow
from core.helper import _ptype, _id
from core.const import *
from core.btree import bt_node, bt_cursor

from dataclasses import dataclass
from utils.logging import info

_info = lambda x: info("executor", x)

class QueryOperator:
    def __init__(self, name, *args):
        self.name = name
        self.args = args

class Equal(QueryOperator):
    def __init__(self, lhs, rhs):
        super(Equal, self).__init__("equal", lhs, rhs)
        self.lhs = lhs 
        self.rhs = rhs

def unwrap(tuple, value):
    match value:
        case get_type("column"):
            return tuple.data[value.pos]
        case get_type("operator"):
            return execute_operator_on_tuple(tuple, value)
        case _ :
            return value

def execute_operator_on_tuple(tuple, operator):
    match operator.name:
        case "equal":
            return unwrap(tuple, operator.lhs) == unwrap(tuple, operator.rhs)

class QueryExecState:
    def __init__(self, table_access, *args, **kwargs):
        self.table_access = table_access
        self.result = None
    
    def on_finisehd(self):
        if self.table_access.obj_lock is not None:
            self.table_access.obj_lock.release()
        
    def set_result(self, res):
        self.result = res

@dataclass
class QueryExecutionCtx:
    xid: int
    allocator: page_allocator
    wal_writer: XLogWriter

class HeapPageInsertState(QueryExecState):
    def __init__(self, ctx: QueryExecutionCtx, table_access, tuple):
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
            raw_update_sys_tables_table_desc(self.table_access.oid, _id(new_root))

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

        if btree_root_page.empty():
            return btree_root_page.insert_tuple_with_init(ctx.allocator, self.tuple, ctx=ctx)

        target_page = btree_root_page
        cursor.visit(target_page)

        while _ptype(target_page) != PAGE_TYPE_DATA:
            target_page_index = target_page.get_internal_node_idx_to_go_down(self.tuple.pk)
            target_page = ref_btree_page(target_page.slots[target_page_index])

        _info(f"tuple_id={self.tuple.pk}, target_page_id={_id(target_page)}, target_page_index={target_page_index}")

        if target_page_index == target_page.key_count + 1 or target_page_index == 0:
            new_heap_page = global_hpalloc()
            new_heap_page.insert(self.tuple, ctx=ctx)
            new_heap_page.mark_min_key(self.tuple.pk)

            split_node, c, _ = target_page.insert_phase_zero(new_heap_page, ctx=ctx)
            assert c.size() == 1

            if split_node is None:
                return target_page

            new_root = bt_node.merge_split_node(self.tuple.pk, cursor, split_node, ctx=ctx)

            if _id(new_root) != _id(btree_root_page):
                raw_update_sys_tables_table_desc(self.table_access.oid, _id(new_root))

        else:
            heap_page_index = target_page.find_leaf_index_to_insert_page(self.tuple.pk)
            heap_page = ref_heap_page(target_page.slots[heap_page_index-1])
            _info(f"insert tuple #{self.tuple.pk} to exisiting heap page #{heap_page.id}")
            insert_with_grow(global_hpalloc, heap_page, self.tuple)

        return 1
    
class BtreePageGetTupleState(QueryExecState):
    def __init__(self, table_access, pk, targets=None, conditions=None):
        super(BtreePageGetTupleState, self).__init__(table_access)

        if conditions is None:
            self.conditions = []
        else:
            self.conditions = conditions
        
        if targets is None:
            self.targets = []
        else:
            self.targets = targets

class CreateIndexState(QueryExecState):
    def __init__(self, table_access, target_col):
        super(CreateIndexState, self).__init__(table_access)
        self.target_col = target_col
    

class BtreePageScanState(QueryExecState):
    def __init__(self, table_access):
        super(BtreePageScanState, self).__init__(table_access)
    
    def exec(self, ctx: QueryExecutionCtx):
        root_page = ref_btree_page(self.table_access.desc_pg_id)
        res = []

        if root_page.empty():
            self.set_result([])
            return True
        
        def scan_heap_page(heap_page):
            data = heap_page.raw_map(lambda buffer: StructuredTuple.parse(buffer), ctx)
            _info(f"collected data from heap page: {data}")
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
    def __init__(self, table_access, targets=None, conditions=None):
        super(HeapPageScanState, self).__init__(table_access)

        self.current_ref_page = None
        self.current_slot_index = 0

        self.ref_pages = []
        self.results = []

        if conditions is None:
            self.conditions = []
        else:
            self.conditions = conditions
        
        if targets is None:
            self.targets = []
        else:
            self.targets = targets
        
    def eval_conditions(self, tuple):
        res = True
        
        for c in self.conditions:
            res = res and unwrap(tuple, c)

        return res 

    def exec(self):
        heap_page = ref_heap_page(self.table_access.desc_pg_id)
        res = []

        while True:
            _res = heap_page.raw_filter(
                f=lambda buffer: StructuredTuple.parse(buffer).struct(self.table_access.schema),
                raw_filter_func=lambda tuple: self.eval_conditions(tuple)
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
        return BtreePageScanState