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

pub enum ColumnsSelect {
    Star,
    ColumnsNames(Vec<String>),
}

pub struct Select {
    columns: ColumnsSelect,
    table: Vec<String>,
}
