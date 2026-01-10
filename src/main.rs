mod value;
use value::Value;

fn main() {
    let v1 = Value::Int(42);
    let v2 = Value::Int(42);
    let v3 = Value::Text("hello".to_string());

    // Clone
    let v1_copy = v1.clone();

    // PartialEq
    println!("v1 == v2 ? {}", v1 == v2); // true
    println!("v1 == v3 ? {}", v1 == v3); // false
    println!("v1 == v1 copy ? {}", v1 == v1_copy); // false

    // Debug
    println!("{:?}", v1);
}
