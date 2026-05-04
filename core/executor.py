from core.catalog import Column, get_type, init_table_access
from core.heap import StructuredTuple
from page_mgr import ref_page

class QueryOperator:
    def __init__(self, name, args):
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
            targets = []
        else:
            self.targets = targets
        
    def eval_conditions(self, tuple):
        res = True
        
        for c in self.conditions:
            res = res and unwrap(tuple, c)

        return res 

    def exec(self):
        heap_page = self.ref_page(self.table_access.desc_pg_id)
        init = True
        res = []

        while init or heap_page.has_next():
            init = False
            _res = heap_page.raw_filter(
                f=lambda buffer: StructuredTuple.parse(buffer).struct(self.table_access.schema),
                raw_filter_func=lambda tuple: self.eveal_conditions(tuple)
            )
            res.extend(_res)
        return res
    
    def add_condition(self, condition):
        self.conditions.append(condition)

def select(namespace, table_oid):
    table_access = init_table_access(namespace, table_oid, lockmode=None)

    #todo match scanstate type acording to table clustered type
    return HeapPageScanState(table_access=table_access)


class HeapPageInsertState(QueryExecState):
    def __init__(self, table_access):
        pass
  