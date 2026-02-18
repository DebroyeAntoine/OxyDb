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
    - `DELETE`: Filtered row removal with index stability.
    - `SELECT`: Projection, filtering, and sorting.
- **Advanced Querying**:
    - **Recursive WHERE clauses**: Supports complex boolean logic (`AND`, `OR`, Comparisons).
    - **ORDER BY**: Multi-column sorting (Ascending/Descending).
    - **LIMIT**: Efficient result set truncation.
- **Efficient Null Handling**: Uses bit-mapped nullability tracking via the `bitvec` crate for zero-overhead null representation.

## 🏗 Architecture

OxyDB follows a modern database pipeline:
1. **Tokenizer**: A hand-written lexical scanner converting SQL strings into typed tokens.
2. **Parser**: A recursive descent parser that builds an Abstract Syntax Tree (AST).
3. **Execution Engine**: Orchestrates data movement and applies filtering logic.
4. **Columnar Storage Layer**: Physical storage using specialized vectors (`Int`, `Float`, `Text`, `Bool`) and null bitmaps.

## 🚀 Quick Start

### Usage Example

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

    Ok(())
}
## Development

### Running Tests

```bash
cargo test
```

### Generating Documentation

```bash
cargo doc --open
```

## Roadmap

- [ ] Joins: Implement Nested Loop Joins for multi-table queries.

- [ ] Aggregations: Add SUM, COUNT, AVG, and MIN/MAX support.

- [ ] Persistence: Implement a Write-Ahead Log (WAL) or snapshotting to disk.

- [ ] Query Optimization: Basic projection pushdown and index-based lookups.

- [ ] In-Memory Indexes: Hash maps or B-Trees for O(1)/O(log n) searches.
- [ ] **String Interning**: Implement a global string pool to ensure that identical string literals across different `INSERT` statements share a single memory allocation, further reducing memory footprint.

## License

Distributed under the MIT License. See `LICENSE` for more information.
