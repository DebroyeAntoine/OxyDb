use crate::{ColumnDef, Value};

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable(CreateTable),
    InsertInto(InsertInto),
    Select(Select),
}

#[derive(Debug, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, PartialEq)]
pub struct InsertInto {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Value>,
}

#[derive(Debug, PartialEq)]
pub enum ColumnsSelect {
    Star,
    ColumnsNames(Vec<String>),
}

#[derive(Debug, PartialEq)]
pub struct Select {
    pub columns: ColumnsSelect,
    pub table: String,
}
