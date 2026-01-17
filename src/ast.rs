use crate::{ColumnDef, Value};

pub enum Statement {
    CreateTable(CreateTable),
    InsertInto(InsertInto),
    Select(Select),
}

pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

pub struct InsertInto {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Value>,
}

pub enum ColumnsSelect {
    Star,
    ColumnsNames(Vec<String>),
}

pub struct Select {
    pub columns: ColumnsSelect,
    pub table: String,
}
