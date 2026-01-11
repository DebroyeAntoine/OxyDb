mod column;
mod data_type;
mod database;
mod table;
mod value;

use column::Column;
use data_type::DataType;
use value::Value;

fn main() {
    let mut col = Column::new("age".to_string(), DataType::Int);

    col.push(Value::Int(30)).unwrap();
    col.push(Value::Null).unwrap();
    col.push(Value::Int(25)).unwrap();

    assert_eq!(col.get(0), Some(Value::Int(30)));
    assert_eq!(col.get(1), Some(Value::Null));
    assert_eq!(col.get(2), Some(Value::Int(25)));
    assert_eq!(col.get(999), None); // Out of bounds

    println!("✅ All tests passed!");
}
