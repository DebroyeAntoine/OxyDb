use std::sync::Arc;

use db::*;

fn main() -> Result<(), String> {
    println!("In-Memory Database Demo\n");

    // Create  DB
    let mut db = database::Database::new();

    // Create table "users"
    let schema = table::Schema {
        columns: vec![
            table::ColumnDef {
                name: "id".into(),
                data_type: data_type::DataType::Int,
            },
            table::ColumnDef {
                name: "name".into(),
                data_type: data_type::DataType::Text,
            },
            table::ColumnDef {
                name: "age".into(),
                data_type: data_type::DataType::Int,
            },
        ],
    };

    db.create_table("users".into(), schema)?;
    println!("Created table 'users'");

    // Insert data
    println!("Inserting data...");
    {
        let table = db.get_table_mut("users").unwrap();

        table.insert(vec![
            Value::Int(1),
            Value::Text(Arc::from("Alice")),
            Value::Int(30),
        ])?;

        table.insert(vec![
            Value::Int(2),
            Value::Text(Arc::from("Bob")),
            Value::Null, // Bob's age is unknown
        ])?;

        table.insert(vec![
            Value::Int(3),
            Value::Text(Arc::from("Charlie")),
            Value::Int(25),
        ])?;

        println!("Inserted 3 rows\n");
    }

    // Read and Printing data
    println!("Reading data:");
    println!("{:<5} {:<10} {:<5}", "ID", "NAME", "AGE");
    println!("{}", "-".repeat(25));

    let table = db.get_table("users").unwrap();

    for row_idx in 0..table.row_count {
        let row = table.get_row(row_idx).unwrap();

        let id = match &row[0] {
            Value::Int(i) => i.to_string(),
            _ => "?".into(),
        };

        let name = match &row[1] {
            Value::Text(s) => s.to_string(),
            _ => "NULL".into(),
        };

        let age = match &row[2] {
            Value::Int(i) => i.to_string(),
            Value::Null => "NULL".into(),
            _ => "?".into(),
        };

        println!("{:<5} {:<10} {:<5}", id, name, age);
    }

    println!();

    // List tables
    println!("Tables in database:");
    for table_name in db.list_tables() {
        println!("  - {}", table_name);
    }

    Ok(())
}
