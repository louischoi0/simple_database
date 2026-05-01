# Simple Database built with Python
this project is PoC (proof of concept) for kernel integrated database system. the goal is to fully understand how each essential component of database is implemented, and works. 


## Database Internal key concept
1. Btree Data structure
2. Lock and Race Condition 
3. Atomicity
4. Cacheing


## Files

### page_mgr.py
- managing page and page cache pool, drop or create pages

### heap.py
- a page type for storing tuple
- tuples contained in heap page is not sorted
- it has slots to indicate each start position for each tuples

### page.py
- basic unit for inserting data which has many types (heap, btree, hash)
- super class has buffer mapped to disk segment 

### btree.py
- data structure for index heap pages to help scan tuples by range or random access

### meta.py
- a component to deal with meta information such as total_page_count, last_committed_lsn ... etc

### catalog.py
- a section for definition: primitive types, sys tables, indexes, schema, columns


