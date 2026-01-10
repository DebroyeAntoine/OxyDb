mod column;
mod data_type;
mod value;

use column::Column;
use data_type::DataType;
use value::Value;

fn main() {
    let mut col = Column::new("age".to_string(), DataType::Int);

    col.push(Value::Int(30)).unwrap();
    col.push(Value::Null).unwrap();
    col.push(Value::Int(25)).unwrap();

    println!("Column length: {}", col.len());
    println!("{:?}", col);
}
