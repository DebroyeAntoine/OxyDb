use db::{Database, Value};
use std::{fs, sync::Arc};

fn main() {
    let n = 100_000;
    let mut db = Database::new();

    db.execute("CREATE TABLE products (id INT, category TEXT)")
        .unwrap();

    let categories = [
        "CATEGORY_LONG_STRING_NUMBER_1_MAX_RAM_USAGE_TESTING",
        "CATEGORY_LONG_STRING_NUMBER_2_MAX_RAM_USAGE_TESTING",
        "CATEGORY_LONG_STRING_NUMBER_3_MAX_RAM_USAGE_TESTING",
        "CATEGORY_LONG_STRING_NUMBER_4_MAX_RAM_USAGE_TESTING",
        "CATEGORY_LONG_STRING_NUMBER_5_MAX_RAM_USAGE_TESTING",
    ];

    println!("Insert n {} lines (5 differents categories)...", n);

    for i in 0..n {
        let cat = categories[i % 5];
        let val_text = Value::Text(Arc::from(cat));

        let table = db.get_table_mut("products").unwrap();
        table.insert(vec![Value::Int(i as i64), val_text]).unwrap();
    }

    let total_size = allocative::size_of_unique_allocated_data(&db);

    println!("\n=== REPORT ===");
    println!("Number of lines : {}", n);
    println!("Total size on the heap : {} bytes", total_size);
    println!("Mean per line : {:.2} bytes", total_size as f64 / n as f64);

    let mut builder = allocative::FlameGraphBuilder::default();
    builder.visit_root(&db);

    let svg_data = builder.finish_and_write_flame_graph();

    let output_path = "heap_report.txt";
    fs::write(output_path, svg_data).expect("Error when writing output");

    println!("\nGenerated report : {}", output_path);
}
