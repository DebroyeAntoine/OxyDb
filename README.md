# OxyDB 🦀 ⚡

OxyDB is a high-performance, in-memory SQL database engine written in Rust. It utilizes **columnar storage** to optimize memory locality and analytical query performance.

[![Rust](https://github.com/DebroyeAntoine/OxyDb/actions/workflows/rust.yml/badge.svg)](https://github.com/DebroyeAntoine/OxyDb/actions/workflows/rust.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## ✨ Features

- **Columnar Storage**: Data is stored by columns rather than rows, maximizing CPU cache hits and reducing memory overhead.
- **SQL Interface**: Includes a hand-written Tokenizer and Recursive Descent Parser supporting `CREATE TABLE`, `INSERT INTO`, and `SELECT`.
- **Efficient Null Handling**: Uses bit-mapped nullability tracking via the `bitvec` crate to avoid memory bloat.
- **Strongly Typed**: Strict type checking during insertion based on the table schema.
- **Safe & Concurrent**: Built with 100% safe Rust, utilizing `Arc<str>` for efficient thread-safe string management.

## 🏗 Architecture

OxyDB follows a modern database pipeline:
1. **Tokenizer**: Lexical analysis of SQL strings into typed tokens.
2. **Parser**: Transformation of tokens into an Abstract Syntax Tree (AST).
3. **Execution Engine**: Logic for table management and data orchestration.
4. **Columnar Storage**: Physical layer using specialized vectors and null bitmaps for optimal performance.

## 🚀 Quick Start

### Usage Example

```rust
use db::{Database, Value};

fn main() -> Result<(), String> {
    let mut db = Database::new();

    // Create a table using SQL
    db.execute("CREATE TABLE users (id INT, name TEXT, age INT)")?;

    // Insert data
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;

    // Query data
    let result = db.query("SELECT name, age FROM users")?;
    
    for row in result.rows {
        println!("Name: {:?}, Age: {:?}", row[0], row[1]);
    }

    Ok(())
}
```

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

- [ ] Implement WHERE clause
- [ ] Add support for UPDATE and DELETE
- [ ] Implement basic Query Optimization (Projection pushdown)
- [ ] Add SUM, COUNT, and AVG aggregate functions.

## License

Distributed under the MIT License. See `LICENSE` for more information.
