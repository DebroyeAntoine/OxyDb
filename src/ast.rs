use crate::{ColumnDef, Value};

/// Represents the top-level SQL statements supported by the database.
/// This enum is the entry point of the Abstract Syntax Tree (AST).
#[derive(Debug, PartialEq)]
pub enum Statement {
    /// An instruction to create a new table schema.
    CreateTable(CreateTable),
    /// An instruction to insert new rows into an existing table.
    InsertInto(InsertInto),
    /// An instruction to query and retrieve data from a table.
    Select(Select),
}

/// Data structure representing a `CREATE TABLE` SQL statement.
/// It defines the table's identity and the structure of its columns.
#[derive(Debug, PartialEq)]
pub struct CreateTable {
    /// The unique name of the table to be created.
    pub name: String,
    /// A list of column definitions, including names, data types, and constraints.
    pub columns: Vec<ColumnDef>,
}

/// Data structure representing an `INSERT INTO` SQL statement.
/// Used to populate a table with new data.
#[derive(Debug, PartialEq)]
pub struct InsertInto {
    /// The name of the target table where data will be inserted.
    pub table: String,
    /// An optional list of column names to target.
    /// If `None`, values are expected to match the table's schema order.
    pub columns: Option<Vec<String>>,
    /// The actual data values to be stored in the new row.
    pub values: Vec<Value>,
}

/// Defines which columns should be retrieved in a `SELECT` query.
#[derive(Debug, PartialEq)]
pub enum ColumnsSelect {
    /// Represents the `*` wildcard, indicating all columns should be returned.
    Star,
    /// Represents a specific list of column names to be returned.
    ColumnsNames(Vec<String>),
}

/// Data structure representing a `SELECT` SQL statement.
/// Used to define what data to fetch and from which source.
#[derive(Debug, PartialEq)]
pub struct Select {
    /// The projection of the query (which columns to include in the result).
    pub columns: ColumnsSelect,
    /// The name of the table to query data from.
    pub table: String,
}
