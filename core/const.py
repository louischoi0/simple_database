PAGE_SIZE = 1024 * 8
BYTE_ORDER = "little"
HDR_SIZE = 24
MAX_PAGE_COUNT = 256
META_SIZE = 1024

PAGE_TYPE_ROOT = 1
PAGE_TYPE_INTERNAL = 2
PAGE_TYPE_DATA = 3
PAGE_TYPE_HEAP = 4


PAGE_MAX_SYS_ID = 200

# actual zero page is system page for meta information of db, 
# so it is okay to treat zero num as null page id in case that sub component deal with except meta system
NULL_PAGE = 0

MAX_KEY_COUNT = 2
MAX_SLOT_COUNT = MAX_KEY_COUNT + 1
BTREE_DETAIL_HDR_SIZE = 8 + 8 + (8*(MAX_SLOT_COUNT-1)) + (8 * MAX_SLOT_COUNT)

BUFFER_CURSOR_DEBUG = False

PRIMITIVE_NULL_TYPE_FLAG = 0
PRIMITIVE_INT_TYPE_FLAG = 1
PRIMITIVE_VARCHAR_TYPE_FLAG = 2


