from core.const import *

def _minkey(page):
    if hasattr(page, "page"):
        return page.page.min_key

    return page.min_key

def _id(page):
    if hasattr(page, "page"):
        return page.page.id
    return page.id

def _buffer(page):
    if hasattr(page, "page"):
        assert len(page.page.buffer) == PAGE_SIZE
        return page.page.buffer

    assert len(page.buffer) == PAGE_SIZE
    return page.buffer

def _ptype(page):
    if hasattr(page, "page"):
        return page.page.type
    return page.type

def _checksum(page):
    if hasattr(page, "page"):
        return page.page.checksum
    return page.checksum