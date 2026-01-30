use crate::{ColumnDef, Value};

pub enum Statement {
    CreateTable(CreateTable),
    InsertInto(InsertInto),
    Select(Select),
}

#[derive(Debug)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug)]
pub struct InsertInto {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Value>,
}

#[derive(Debug)]
pub enum ColumnsSelect {
    Star,
    ColumnsNames(Vec<String>),
}

#[derive(Debug)]
pub struct Select {
    pub columns: ColumnsSelect,
    pub table: String,
}
