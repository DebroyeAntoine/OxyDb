use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use db::{Database, Value};
use std::hint::black_box;
use std::sync::Arc;

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

fn bench_delete_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("Delete_Performance");

    for n in [1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, &n| {
            b.iter_with_setup(
                || setup_populated_db(n),
                |mut db| {
                    db.execute("DELETE FROM users WHERE age > 90").unwrap();
                    black_box(db);
                },
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_insert_sql,
    bench_select_scaling,
    bench_update_performance,
    bench_delete_performance
);
criterion_main!(benches);
