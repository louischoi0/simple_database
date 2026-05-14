from core.meta import _init_meta_system, get_metablock
from core.blk import _init_blk_driver


if __name__ == "__main__":
    driver_num = 0

    blk = _init_blk_driver(driver_num)
    meta = _init_meta_system(blk)
    #meta.bootstrap()
    meta = get_metablock()
    meta.init()
    print(meta)
    print(meta.meta_page.checksum())

