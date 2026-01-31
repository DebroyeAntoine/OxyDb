//! # Database Demo
//!
//! This example demonstrates the full end-to-end flow of the database:
//! 1. Initializing the engine.
//! 2. Executing SQL DDL (Data Definition Language) to create tables.
//! 3. Executing SQL DML (Data Manipulation Language) to insert data.
//! 4. Querying data using SQL and displaying the results.

use db::{Database, Value};

fn main() -> Result<(), String> {
    println!("--- In-Memory Database SQL Demo ---\n");

    // Initialize the database engine.
    // We use the re-exported Database type from the crate root.
    let mut db = Database::new();

    // 1. Create a table using a raw SQL string.
    // This triggers the Tokenizer and Parser internally.
    println!("Step 1: Creating table...");
    db.execute("CREATE TABLE users (id INT, name TEXT, age INT)")?;
    println!("Table 'users' created successfully.\n");

    // 2. Insert data using SQL.
    // We show different ways to insert: standard order, reordered columns, and partial columns (NULLs).
    println!("Step 2: Inserting data...");
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")?;
    db.execute("INSERT INTO users (name, id) VALUES ('Bob', 2)")?; // Order doesn't matter
    db.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')")?; // Age will be NULL
    println!("3 rows inserted.\n");

    // 3. Query the data.
    // The query method returns a QueryResult containing column names and rows.
    println!("Step 3: Querying data (SELECT * FROM users):");
    let result = db.query("SELECT * FROM users")?;

    // Display the header
    for col_name in &result.columns {
        print!("{:<10} ", col_name.to_uppercase());
    }
    println!("\n{}", "-".repeat(result.columns.len() * 11));

    // Display the rows
    for row in result.rows {
        for value in row {
            let display = match value {
                Value::Int(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Text(s) => s.to_string(),
                Value::Bool(b) => b.to_string().to_uppercase(),
                Value::Null => "NULL".to_string(),
            };
            print!("{:<10} ", display);
        }
        println!();
    }

    // 4. List metadata.
    println!("\nStep 4: Database Metadata:");
    let tables = db.list_tables();
    println!("Existing tables: {:?}", tables);

    println!("\nDemo completed successfully.");
    Ok(())
}
