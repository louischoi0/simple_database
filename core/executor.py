from core.catalog import Column, get_type, init_table_access
from core.heap import StructuredTuple
from core.page_mgr import ref_page, ref_heap_page, global_hpalloc, ref_btree_page
from core.catalog import get_table_schema_from_cache, is_table_clustered_heap, is_table_clustered_btree
from core.heap import insert_with_grow
from core.helper import _ptype
from const import *

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

class HeapPageInsertState(QueryExecState):
    def __init__(self, table_access, tuple):
        super(HeapPageInsertState, self).__init__(table_access)
        self.tuple = tuple

    def exec(self):
        assert is_table_clustered_heap(self.table_access)

        heap_page = ref_heap_page(self.table_access.desc_pg_id)
        insert_with_grow(global_hpalloc, heap_page, self.tuple)
        return 1
        

class BtreePageInsertState(QueryExecState):
    def __init__(self, table_access, tuple):
        super(BtreePageInsertState, self).__init__(table_access)
        self.tuple = tuple

    def exec(self):
        assert is_table_clustered_btree(self.table_access)

        btree_root_page = ref_btree_page(self.table_access.desc_pg_id)
        target_page = btree_root_page

        while _ptype(target_page) != PAGE_TYPE_DATA:
            target_page_index = target_page.find_leaf_index_to_insert(self.tuple.pk)
            target_page = ref_btree_page(target_page_index)

        if target_page_index == target_page.key_count + 1 or target_page_index == 0:
            new_heap_page = global_hpalloc()
            new_heap_page.insert(self.tuple)
            new_heap_page.mark_min_key(self.tuple.pk)
            return target_page.insert(new_heap_page)

        else:
            heap_page_index = target_page.find_leaf_index_to_insert(self.tuple.pk)
            heap_page = ref_heap_page(target_page.slots[heap_page_index])
            insert_with_grow(global_hpalloc, heap_page, self.tuple)

        return 1

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
    table_access = init_table_access(namespace, table_oid, lockmode=None)
    schema = get_table_schema_from_cache(table_oid)
    data_tuple = StructuredTuple.load(schema, raw_data)

    return HeapPageInsertState(table_access=table_access, tuple=data_tuple)

def init_select(namespace, table_oid):
    table_access = init_table_access(namespace, table_oid, lockmode=None)

    #todo match scanstate type acording to table clustered type
    return HeapPageScanState(table_access=table_access)
