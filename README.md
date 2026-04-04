# OxyDB 🦀 ⚡

OxyDB is a high-performance, in-memory SQL database engine written in Rust. It utilizes **columnar storage** to optimize memory locality and analytical query performance.

[![Rust CI](https://github.com/DebroyeAntoine/OxyDb/actions/workflows/rust.yaml/badge.svg)](https://github.com/DebroyeAntoine/OxyDb/actions/workflows/rust.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Project Purpose

OxyDB is an educational project developed to master the Rust programming language and explore the inner workings of database internals. The primary goal is to understand how a high-level SQL interface maps to low-level memory management and efficient data structures.

**Project Philosophy:**
- **Hand-crafted Core:** The Tokenizer, Parser, and Execution Engine are 100% hand-written from scratch to avoid "black-box" dependencies.
- **AI-Enhanced Documentation:** While the logic is purely human-authored, documentation and comments have been refined with the assistance of AI to ensure clarity.

## ✨ Features

- **Columnar Storage**: Data is stored by columns rather than rows, maximizing CPU cache hits and minimizing memory footprint.
- **Complete DDL/DML Support**:
    - `CREATE TABLE`: Schema definition with strict typing.
    - `INSERT INTO`: Positional or named column insertion.
    - `UPDATE`: Mass updates with shared memory optimization for strings (`Arc<str>`).
    - `DELETE`: O(1) logical deletion via a deletion vector bitmap. Rows are marked as dead without any memory movement.
    - `SELECT`: Projection, filtering, and sorting. Dead rows are transparently skipped.
    - `VACUUM`: Physical compaction that permanently removes dead rows and reclaims memory. Can target a single table (`VACUUM users`) or all tables (`VACUUM`).
- **Aggregate Functions**: `COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX` — with full `GROUP BY` support. Groups are partitioned via a `BTreeMap` keyed on group column values; `NULL` inputs follow SQL standard semantics (ignored by `SUM`/`AVG`/`MIN`/`MAX`, counted as 0 by `COUNT(*)`).
- **Advanced Querying**:
    - **Recursive WHERE clauses**: Supports complex boolean logic (`AND`, `OR`, Comparisons).
    - **ORDER BY**: Multi-column sorting (Ascending/Descending).
    - **LIMIT**: Efficient result set truncation.
- **Efficient Null Handling**: Uses bit-mapped nullability tracking via the `bitvec` crate for zero-overhead null representation.
- **Auto-Vacuum**: Configurable automatic compaction triggered after `DELETE` operations based on absolute row count and deletion ratio thresholds.

## 🏗 Architecture

OxyDB follows a modern database pipeline:
1. **Tokenizer**: A hand-written lexical scanner converting SQL strings into typed tokens.
2. **Parser**: A recursive descent parser that builds an Abstract Syntax Tree (AST).
3. **Execution Engine**: Orchestrates data movement and applies filtering logic.
4. **Columnar Storage Layer**: Physical storage using specialized vectors (`Int`, `Float`, `Text`, `Bool`) and null bitmaps.
5. **Deletion Vector**: A per-table `BitVec` that tracks logically deleted rows, enabling O(1) deletes and deferred physical compaction.

## 🗑️ Deletion Model

OxyDB uses a **two-phase deletion** strategy inspired by modern analytical engines like DuckDB and Delta Lake:

1. **Logical delete (O(1))**: `DELETE` marks rows as dead in a per-table `deletion_vector` bitmap. No data is moved.
2. **Physical compaction**: `VACUUM` rebuilds column vectors by copying only the live rows, reclaiming memory in a single pass. The work is proportional to the number of *surviving* rows, not the total — compacting a table with 90% dead rows is cheaper than one with 10% dead rows.

This model provides a **×11 improvement** over immediate physical deletion on 10k rows (571µs vs 6.8ms).

### Auto-Vacuum

Auto-vacuum triggers automatically after each `DELETE` when both thresholds are exceeded:

```rust
db.vacuum_config.min_deleted_rows = 500;  // absolute threshold
db.vacuum_config.deleted_ratio = 0.25;    // 25% of table must be dead
db.vacuum_config.enabled = true;          // can be disabled
```

The overhead of the threshold check when vacuum does not trigger is negligible (<1µs).

## 📊 Performance

Benchmarks run on 10k rows, 4 columns (`INT`, `TEXT`, `INT`, `BOOL`):

| Operation | Time (10k rows) | Complexity | Notes |
|---|---|---|---|
| INSERT (SQL pipeline) | ~499 ns/row | O(1) | Includes tokenizer + parser |
| SELECT with WHERE | 449 µs | O(n) | Direct column reference, no materialization |
| UPDATE (50% rows) | 605 µs | O(n) | In-place value replacement |
| DELETE (logical) | 571 µs | O(n) | Bit-set only, no memory movement |
| DELETE (old physical) | 6.8 ms | O(k·n) | Removed — kept for reference |
| VACUUM (10% dead) | 333 µs | O(survivors) | Copies 9k rows |
| VACUUM (90% dead) | 208 µs | O(survivors) | Copies only 1k rows |

Memory benchmark on 100k rows, 2 columns (`INT`, `TEXT`), 5 distinct string values: **3MB heap** thanks to string interning (vs ~10MB without).

## 🚀 Quick Start

```rust
use db::{Database, Value};

fn main() -> Result<(), String> {
    let mut db = Database::new();

    // Setup table
    db.execute("CREATE TABLE users (id INT, name TEXT, age INT)")?;

    // Insert data
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")?;

    // Update and Delete
    db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
    db.execute("DELETE FROM users WHERE age < 18")?;

    // Query with sorting and filtering
    let result = db.query("SELECT name, age FROM users WHERE age > 20 ORDER BY age DESC LIMIT 10")?;

    for row in result.rows {
        println!("Name: {:?}, Age: {:?}", row[0], row[1]);
    }

    // Reclaim memory from deleted rows
    db.execute("VACUUM")?;

    Ok(())
}
```

## 🛠 Development

### Running Tests

```bash
cargo test
```

### Running Benchmarks

```bash
cargo bench
```

### Running heap Benchmarks
```bash
cargo run --example mem_check
```
And open the generated file in speedscope to see the flamegraph

### Generating Documentation

```bash
cargo doc --open
```

## Roadmap

- [x] **Deletion Vectors**: O(1) logical delete with deferred physical compaction via `VACUUM`.
- [x] **Auto-Vacuum**: Configurable automatic compaction with ratio and absolute count thresholds.
- [x] **String Interning**: Implement a global string pool so identical string literals share a single memory allocation, reducing `Arc` overhead across large datasets.
- [x] **Aggregations**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` with `GROUP BY` support.
- [ ] **In-Memory Indexes**: Hash maps or B-Trees for O(1)/O(log n) lookups, with a query planner to choose between index scan and sequential scan.
- [ ] **Joins**: Nested Loop Join first, then Hash Join for larger datasets.
- [ ] **Persistence**: Write-Ahead Log (WAL) for crash recovery, plus columnar snapshot format for full reload on startup.
- [ ] **Replication**: Master/slave architecture over the network — slaves replicate DDL/DML from master via WAL shipping, with per-table read permissions and the ability to create local-only tables.

## License

Distributed under the MIT License. See `LICENSE` for more information.
