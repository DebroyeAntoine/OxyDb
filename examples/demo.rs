//! # OxyDB SQL Demo
//!
//! This example demonstrates the full capabilities of the OxyDB engine:
//! 1. Initializing the engine and creating tables.
//! 2. Advanced data insertion (named columns, partial data).
//! 3. Data modification using UPDATE.
//! 4. Row removal using DELETE with filtering.
//! 5. Complex querying using WHERE, ORDER BY, and LIMIT.

use db::{Database, Value};

fn main() -> Result<(), String> {
    println!("--- OxyDB In-Memory SQL Demo ---\n");

    let mut db = Database::new();

    // 1. DDL: Create a table
    println!("Step 1: Creating table 'users'...");
    db.execute("CREATE TABLE users (id INT, name TEXT, age INT, active BOOL)")?;

    // 2. DML: Insert data
    println!("Step 2: Inserting records...");
    db.execute("INSERT INTO users (id, name, age, active) VALUES (1, 'Alice', 30, TRUE)")?;
    db.execute("INSERT INTO users (id, name, age, active) VALUES (2, 'Bob', 25, TRUE)")?;
    db.execute("INSERT INTO users (id, name, age, active) VALUES (3, 'Charlie', 40, FALSE)")?;
    db.execute("INSERT INTO users (id, name, age, active) VALUES (4, 'Dave', 19, TRUE)")?;
    db.execute("INSERT INTO users (id, name) VALUES (5, 'Eve')")?; // Partial insert (age & active will be NULL)

    println!("Initial state of 'users' table:");
    let initial_res = db.query("SELECT * FROM users")?;

    // Manual pretty print logic
    print_table(&initial_res.columns, &initial_res.rows);

    // 3. UPDATE: Modify data
    println!("\nStep 3: Updating Alice's age and activating Eve...");
    db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")?;
    db.execute("UPDATE users SET active = TRUE WHERE id = 5")?;

    // 4. DELETE: Remove data
    println!("Step 4: Deleting inactive users (Charlie)...");
    db.execute("DELETE FROM users WHERE active = FALSE")?;

    // 5. Advanced Querying: Filtering, Sorting, and Limiting
    println!("\nStep 5: Executing complex query:");
    println!("SQL: SELECT name, age FROM users WHERE age > 18 ORDER BY age DESC LIMIT 2");

    let result =
        db.query("SELECT name, age FROM users WHERE age > 18 ORDER BY age DESC LIMIT 2")?;
    print_table(&result.columns, &result.rows);

    // 6. Metadata
    println!("\nStep 6: Database Metadata:");
    println!("Existing tables: {:?}", db.list_tables());

    println!("\nDemo completed successfully.");
    Ok(())
}

/// Helper function included in the demo to print results nicely
fn print_table(columns: &[String], rows: &[Vec<Value>]) {
    if rows.is_empty() {
        println!("(empty set)");
        return;
    }

    // Print header
    for col_name in columns {
        print!("{:<15} ", col_name.to_uppercase());
    }
    println!("\n{}", "-".repeat(columns.len() * 16));

    // Print rows
    for row in rows {
        for value in row {
            let display = match value {
                Value::Int(i) => i.to_string(),
                Value::Float(f) => format!("{:.2}", f),
                Value::Text(s) => s.to_string(),
                Value::Bool(b) => b.to_string().to_uppercase(),
                Value::Null => "NULL".to_string(),
            };
            print!("{:<15} ", display);
        }
        println!();
    }
}
