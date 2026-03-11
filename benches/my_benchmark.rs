use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use db::{Database, Value};
use std::hint::black_box;
use std::sync::Arc;

// --- Helpers ---

fn setup_populated_db(n: usize) -> Database {
    let mut db = Database::new();
    db.execute("CREATE TABLE users (id INT, name TEXT, age INT, active BOOL)")
        .unwrap();
    let table = db.get_table_mut("users").unwrap();

    for i in 0..n {
        let row = vec![
            Value::Int(i as i64),
            Value::Text(Arc::from(format!("user{}", i).as_str())),
            Value::Int((i % 100) as i64),
            Value::Bool(i % 2 == 0),
        ];
        table.insert(row).unwrap();
    }
    db
}

fn setup_categorized_db(n: usize, num_categories: usize) -> Database {
    let mut db = Database::new();
    db.execute("CREATE TABLE products (id INT, category TEXT)")
        .unwrap();
    let table = db.get_table_mut("products").unwrap();

    for i in 0..n {
        let cat_name = format!("category{}", i % num_categories);
        let row = vec![
            Value::Int(i as i64),
            Value::Text(Arc::from(cat_name.as_str())),
        ];
        table.insert(row).unwrap();
    }
    db
}

// --- Benchmarks ---

fn bench_delete_logical(c: &mut Criterion) {
    let mut group = c.benchmark_group("Delete_Logical_Performance");
    for n in [1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter_with_setup(
                || {
                    let mut db = setup_populated_db(n);
                    // Disable auto-vacuum to measure ONLY the delete bit-setting
                    db.vacuum_config.enabled = false;
                    db
                },
                |mut db| {
                    // This only sets bits in the deletion_vector
                    db.execute("DELETE FROM users WHERE age > 50").unwrap();
                    black_box(db);
                },
            );
        });
    }
    group.finish();
}

fn bench_vacuum_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("Vacuum_Compaction_Cost");
    let n = 10000; // Fixed size to see the impact of ratio

    for ratio in [0.1, 0.5, 0.9].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}%", ratio * 100.0)),
            ratio,
            |b, &ratio| {
                b.iter_with_setup(
                    || {
                        let mut db = setup_populated_db(n);
                        db.vacuum_config.enabled = false;
                        // Pre-delete rows to create fragmentation
                        let limit = (n as f64 * ratio) as i64;
                        db.execute(&format!("DELETE FROM users WHERE id < {}", limit))
                            .unwrap();
                        db
                    },
                    |mut db| {
                        // Timing the physical rebuild of the columns
                        db.execute("VACUUM users").unwrap();
                        black_box(db);
                    },
                );
            },
        );
    }
    group.finish();
}

fn bench_auto_vacuum_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("Auto_Vacuum_Overhead");
    let n = 5000;

    group.bench_function("delete_without_check", |b| {
        b.iter_with_setup(
            || {
                let mut db = setup_populated_db(n);
                db.vacuum_config.enabled = false; // Check is skipped
                db
            },
            |mut db| {
                db.execute("DELETE FROM users WHERE id = 1").unwrap();
                black_box(db);
            },
        );
    });

    group.bench_function("delete_with_check_not_triggered", |b| {
        b.iter_with_setup(
            || {
                let mut db = setup_populated_db(n);
                db.vacuum_config.enabled = true;
                db.vacuum_config.min_deleted_rows = 1000; // Threshold high so it won't trigger
                db
            },
            |mut db| {
                db.execute("DELETE FROM users WHERE id = 1").unwrap();
                black_box(db);
            },
        );
    });
    group.finish();
}

fn bench_select_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("Select_Where_Performance");
    for n in [1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            let db = setup_populated_db(n);
            b.iter(|| {
                let res = db.query("SELECT * FROM users WHERE age = 42").unwrap();
                black_box(res);
            });
        });
    }
    group.finish();
}

fn bench_update_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("Update_Performance");

    for n in [1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter_with_setup(
                || setup_populated_db(n),
                |mut db| {
                    db.execute("UPDATE users SET age = 99 WHERE active = TRUE")
                        .unwrap();
                    black_box(db);
                },
            );
        });
    }
    group.finish();
}

fn bench_string_update_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("String_Update_Performance");

    for n in [1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter_with_setup(
                || setup_populated_db(n),
                |mut db| {
                    db.execute("UPDATE users SET name = 'StandardName' WHERE active = TRUE")
                        .unwrap();
                    black_box(db);
                },
            );
        });
    }
    group.finish();
}

fn bench_insert_sql(c: &mut Criterion) {
    let mut group = c.benchmark_group("Insert_SQL_Pipeline");
    group.bench_function("insert_single_row_sql", |b| {
        let mut db = Database::new();
        db.execute("CREATE TABLE tests (id INT)").unwrap();
        b.iter(|| {
            db.execute(black_box("INSERT INTO tests VALUES (42)"))
                .unwrap();
        });
    });
    group.finish();
}

fn bench_select_string_interned(c: &mut Criterion) {
    let n = 10000;
    let db = setup_categorized_db(n, 5);

    let mut group = c.benchmark_group("Select_String_Equality");
    group.bench_function("filter_on_interned_string", |b| {
        b.iter(|| {
            let res = db
                .query("SELECT * FROM products WHERE category = 'category2'")
                .unwrap();
            black_box(res);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_delete_logical,
    bench_vacuum_scaling,
    bench_auto_vacuum_overhead,
    bench_select_scaling,
    bench_update_performance,
    bench_insert_sql,
    bench_select_string_interned,
    bench_string_update_performance,
);
criterion_main!(benches);
