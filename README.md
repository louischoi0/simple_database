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
- Tuple(Record, Row) 를 저장하는 기본 페이지 단위 입니다.
- min_key를 이용해 heap에 포함된 최소값을 저장하여 인덱싱을 용이하게 합니다.
- heap 페이지 내부는 정렬 되어 있지 않으며 특정 키를 검색하기 위해서는 heap 페이지를 scan합니다. 


### page.py
- 데이터를 저장하는 가장 기본적인 단위로써 페이지가 정의되어 있습니다.
- 동시성을 관리하기 위한 Lock, Pin 등의 기능을 포함하고 있습니다.

### btree.py
- 힙페이지를 인덱싱하는 자료구조 페이지 입니다.

### meta.py
- 현재까지 생성된 페이지 수와 같은 전체적인 데이터베이스 시스템의 상태값을 관리하는 컴포넌트 입니다.

### catalog.py
- 데이터베이스의 원시 타입, 테이블 정보, 인덱스 정보들을 관리하기 위한 시스템 변수들의 선언을 포함합니다.
