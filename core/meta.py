from utils.dec import *
from core.page import cast_page
from core.helper import _id, _buffer

class metablock:
    def __init__(self, max_page):
        self.max_page = max_page
    
    def set_max_page(self, max_page):
        self.max_page = max_page
    
    def inc(self):
        self.max_page += 1
        return self.max_page
