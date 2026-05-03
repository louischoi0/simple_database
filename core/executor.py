from core.catalog import get_table_schema_from_cache

class QueryExecState:
    def __init__(self, *args, **kwargs):
      pass




class HeapPageScanState:
    def __init__(self, entry_heap_page, quals=None, ):
        self.entry_heap_page = entry_heap_page

        self.current_ref_page = None
        self.ref_pages = []



class HeapPageInsertState:
    def __init__(self, entry_heap_page, data_buffer, )
  


class QueryExecutor:
    def __init__(self):
        pass
      
    def insert(self, oid, )

def Select