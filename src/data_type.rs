/// Represents the supported data types in the database schema.
/// These types define the structure of columns and the expected format of values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// A 64-bit signed integer.
    Int,
    /// A 64-bit floating-point number.
    Float,
    /// A variable-length UTF-8 character string.
    Text,
    /// A boolean value (true or false).
    Bool,
}
