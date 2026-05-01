# Simple Database — Built with Python

> **PoC (Proof of Concept)** for a kernel-integrated database system.  
> The goal is to deeply understand how each essential component of a database is implemented and operates — from raw disk I/O to B-tree indexing.

## Architecture Overview
```
┌─────────────────────────────────────────┐
│              catalog.py                 │  Schema, types, sys tables
├─────────────────────────────────────────┤
│               btree.py                  │  Index layer
├─────────────────────────────────────────┤
│               heap.py                   │  Tuple storage (unsorted)
├──────────────────┬──────────────────────┤
│    page_mgr.py   │      meta.py         │  Page cache / Metadata
├──────────────────┴──────────────────────┤
│               page.py                   │  Base page abstraction
└─────────────────────────────────────────┘
```

## Key Concepts

| Concept | Description |
|---|---|
| **B-tree** | Index structure for range and random access |
| **Locking & Race Conditions** | Concurrency control for safe multi-access |
| **Atomicity** | Transaction guarantees — all-or-nothing writes |
| **Caching** | Page cache pool to reduce disk I/O |

## Module Reference

### `page.py`
Base class for all page types. Holds a buffer that is mapped directly to a disk segment. Subclassed by `heap`, `btree`, and `hash` page implementations.

### `page_mgr.py`
Manages the page cache pool. Responsible for allocating and dropping pages, and coordinating access between in-memory buffers and disk.

### `heap.py`
A page type for storing tuples. Tuples are **unsorted** and accessed via a slot array that tracks the start offset of each tuple within the page.

### `btree.py`
B-tree implementation used to index heap pages. Supports both range scans and random access lookups by key.

### `meta.py`
Handles database-level metadata including `total_page_count`, `last_committed_lsn`, and other system-wide state.

### `catalog.py`
Defines the database schema layer: primitive types, system tables, indexes, column definitions, and schema objects.
