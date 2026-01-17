use crate::{ColumnDef, Value};

pub enum Statement {
    CreateTable(CreateTable),
    InsertInto(InsertInto),
    Select(Select),
}

pub struct CreateTable {
    name: String,
    columns: Vec<ColumnDef>,
}

pub struct InsertInto {
    table: String,
    values: Vec<Value>,
}

pub struct Select {
    columns: Vec<String>,
    table: Vec<String>,
}
