from core.catalog import get_table_schema_from_cache
from page_mgr import ref_page

class QueryOperator:
    def __init__(self, name, args):
        self.name = name
        self.args = args

class QueryExecState:
    def __init__(self, table_access, *args, **kwargs):
        self.table_access = table_access

class HeapPageScanState(QueryExecState):
    def __init__(self, table_access, targets=None, quals=None):
        super(HeapPageScanState, self).__init__(table_access)

        self.current_ref_page = None
        self.current_slot_index = 0

        self.ref_pages = []
        self.results = []

        if quals is None:
            quals = []
        else:
            self.quals = quals
        
        if targets is None:
            targets = []
        else:
            self.targets = targets
    
    def exec(self):
        table_desc_pg = self.ref_page(self.table_access.desc_pg_id)
        init = True

        while init or table_desc_pg.has_next():
            init = False





class HeapPageInsertState(QueryExecState):
    def __init__(self, table_access):
        pass
  