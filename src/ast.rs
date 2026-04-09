use std::collections::HashMap;

use allocative::Allocative;

use crate::{ColumnDef, Value};

/// Represents the top-level SQL statements supported by the database.
/// This enum is the entry point of the Abstract Syntax Tree (AST).
#[derive(Debug, PartialEq, Allocative)]
pub enum Statement<'a> {
    /// An instruction to create a new table schema.
    CreateTable(CreateTable<'a>),
    /// An instruction to insert new rows into an existing table.
    InsertInto(InsertInto<'a>),
    /// An instruction to query and retrieve data from a table.
    Select(Select<'a>),
    /// An instruction to delete data matching the query.
    Delete(Delete<'a>),
    /// An instruction to update data mathcing the query.
    Update(Update<'a>),
    /// An instruction to delete values marked as deleted for a table.
    /// If no table is given, it will vacuum all tables.
    #[allocative(skip)]
    Vacuum(Option<&'a str>),
}

/// Data structure representing a `CREATE TABLE` SQL statement.
/// It defines the table's identity and the structure of its columns.
#[derive(Debug, PartialEq, Allocative)]
pub struct CreateTable<'a> {
    /// The unique name of the table to be created.
    #[allocative(skip)]
    pub name: &'a str,
    /// A list of column definitions, including names, data types, and constraints.
    pub columns: Vec<ColumnDef>,
}

/// Data structure representing an `INSERT INTO` SQL statement.
/// Used to populate a table with new data.
#[derive(Debug, PartialEq, Allocative)]
pub struct InsertInto<'a> {
    /// The name of the target table where data will be inserted.
    #[allocative(skip)]
    pub table: &'a str,
    /// An optional list of column names to target.
    /// If `None`, values are expected to match the table's schema order.
    #[allocative(skip)]
    pub columns: Option<Vec<&'a str>>,
    /// The actual data values to be stored in the new row.
    pub values: Vec<Value>,
}

/// Defines which columns should be retrieved in a `SELECT` query.
#[derive(Debug, PartialEq, Allocative)]
pub enum ColumnsSelect<'a> {
    /// Represents the `*` wildcard, indicating all columns should be returned.
    Star,
    /// Represents a specific list of Items to be returned.
    Items(Vec<SelectItem<'a>>),
}

/// Data structure representing a `SELECT` SQL statement.
/// Used to define what data to fetch and from which source.
#[derive(Debug, PartialEq, Allocative)]
pub struct Select<'a> {
    /// The projection of the query (which columns to include in the result).
    pub columns: ColumnsSelect<'a>,
    /// The name of the table to query data from.
    #[allocative(skip)]
    pub table: &'a str,

    /// Where clause optionnal
    pub where_clause: Option<Expr<'a>>,

    // Limit the number or rows to be shown optionnal
    pub limit: Option<usize>,

    // Order by clause optionnal
    pub order_by: Option<Vec<OrderByClause<'a>>>,

    #[allocative(skip)]
    pub group_by: Option<Vec<&'a str>>,
}

#[derive(Debug, PartialEq, Clone, Allocative)]
pub enum Aggregate<'a> {
    /// SUM(column)
    #[allocative(skip)]
    Sum(&'a str),
    /// AVG(column)
    #[allocative(skip)]
    Avg(&'a str),
    /// MIN(column)
    #[allocative(skip)]
    Min(&'a str),
    /// MAX(column)
    #[allocative(skip)]
    Max(&'a str),
    /// COUNT(*)
    CountStar,
    /// COUNT(column)
    #[allocative(skip)]
    Count(&'a str),
}

#[derive(Debug, PartialEq, Clone, Allocative)]
pub enum SelectItem<'a> {
    /// A simple column
    #[allocative(skip)]
    Column(&'a str),
    /// Aggregation: `SUM(price)` or `COUNT(*)`
    Aggregate(Aggregate<'a>),
}

/// Data structure representing a `DELETE` SQL statement.
/// Used to define which rows to be deleted in a specific table
#[derive(Debug, PartialEq, Allocative)]
pub struct Delete<'a> {
    /// The name of the table to query data from.
    #[allocative(skip)]
    pub table: &'a str,

    /// Where clause
    pub where_clause: Expr<'a>,
}

/// Represents a boolean comparison operation between a column and a literal value.
#[derive(Debug, PartialEq, Allocative)]
pub enum ComparisonOp {
    /// Greater than (`>`)
    Gt,
    /// Lower than (`<`)
    Lt,
    /// Equal (`=`)
    Eq,
}

/// Represents a single sorting criterion in an `ORDER BY` clause.
///
/// # Examples
/// ```sql
/// ORDER BY age ASC        -- Single clause
/// ORDER BY age DESC, name ASC  -- Multiple clauses (age first, then name)
/// ```
#[derive(Debug, PartialEq, Clone, Allocative)]
pub struct OrderByClause<'a> {
    /// The name of the column to sort by.
    #[allocative(skip)]
    pub column: &'a str,

    /// The sort direction (ascending or descending).
    pub direction: SortDirection,
}

/// Defines the direction of sorting for an `ORDER BY` clause.
#[derive(Debug, PartialEq, Clone, Copy, Allocative)]
pub enum SortDirection {
    /// Sort in ascending order (smallest to largest, A-Z).
    /// This is the SQL default if no direction is specified.
    Asc,

    /// Sort in descending order (largest to smallest, Z-A).
    Desc,
}

/// A recursive expression tree used in `WHERE` clauses to filter rows.
#[derive(Debug, PartialEq, Allocative)]
pub enum Expr<'a> {
    /// A leaf node: compares a specific column to a constant value.
    Comparison {
        /// The name of the column to evaluate.
        #[allocative(skip)]
        column: &'a str,
        /// The operator to apply.
        op: ComparisonOp,
        /// The constant value to compare against.
        value: Value,
    },
    /// A logical AND operation. Both sides must be true.
    And {
        left: Box<Expr<'a>>,
        right: Box<Expr<'a>>,
    },
    /// A logical OR operation. At least one side must be true.
    Or {
        left: Box<Expr<'a>>,
        right: Box<Expr<'a>>,
    },
}

/// Data structure representing an `UPDATE` SQL statement.
/// Used to modify existing rows in a table.
#[derive(Debug, PartialEq, Allocative)]
pub struct Update<'a> {
    /// The name of the table to update.
    #[allocative(skip)]
    pub table: &'a str,

    /// List of column assignments (column_name, new_value).
    /// The order doesn't matter as each assignment is independent.
    #[allocative(skip)]
    pub assignments: HashMap<&'a str, Value>,

    /// WHERE clause to filter which rows to update.
    /// All matching rows will have their specified columns updated.
    pub where_clause: Expr<'a>,
}
