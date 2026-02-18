use crate::{
    ColumnDef, Value,
    ast::{
        ColumnsSelect, ComparisonOp, Delete, Expr, InsertInto, OrderByClause, SortDirection,
        Statement, Update,
    },
    parser::Parser,
    table::{Schema, Table},
    tokenizer::Tokenizer,
};
use std::{cmp::Ordering, collections::HashMap};

/// The main entry point for the in-memory database engine.
/// It manages a collection of tables and orchestrates query execution.
#[derive(Default)]
pub struct Database {
    /// A map of table names to their respective [Table] structures.
    tables: HashMap<String, Table>,
}

/// Represents the result of a successful `SELECT` query.
#[derive(Debug)]
pub struct QueryResult {
    /// The names of the columns included in the result set.
    pub columns: Vec<String>,
    /// The actual data, returned as a vector of rows, where each row is a vector of [Value].
    pub rows: Vec<Vec<Value>>,
}

impl Database {
    /// Creates a new, empty database instance.
    pub fn new() -> Self {
        Self {
            tables: HashMap::default(),
        }
    }

    /// Creates a new table in the database.
    ///
    /// # Errors
    /// Returns an error if a table with the same name already exists.
    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table {} already exists in the database", name));
        }
        let table = Table::new(name.clone(), schema);
        self.tables.insert(name, table);
        Ok(())
    }

    /// Removes a table from the database by its name.
    ///
    /// # Errors
    /// Returns an error if the table does not exist.
    pub fn drop_table(&mut self, name: &str) -> Result<(), String> {
        match self.tables.remove(name) {
            Some(_) => Ok(()),
            None => Err(format!("Table {} does not exist in the database", name)),
        }
    }

    /// Retrieves a reference to a table by name.
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Retrieves a mutable reference to a table by name.
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    /// Returns a list of all table names currently stored in the database.
    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.iter().map(|m| m.0.as_str()).collect()
    }

    /// Executes a SQL statement that modifies the database state (DDL/DML).
    ///
    /// This handles `CREATE TABLE` and `INSERT INTO`.
    /// For data retrieval, use [Database::query] instead.
    ///
    /// # Errors
    /// Returns an error if tokenization, parsing, or execution fails.
    ///
    /// # Example
    /// ```
    /// use db::{Database, Value};
    /// let mut db = Database::new();
    /// db.execute("CREATE TABLE users (id INT)").unwrap();
    /// db.execute("INSERT INTO users VALUES (1)").unwrap();
    /// db.execute("DELETE FROM users WHERE id > 12").unwrap();
    ///
    /// let result = db.query("SELECT * FROM users").unwrap();
    /// assert_eq!(result.rows[0][0], Value::Int(1));
    /// ```
    pub fn execute(&mut self, sql: &str) -> Result<(), String> {
        let tokens = Tokenizer::new(sql).tokenize()?;
        let statement = Parser::new(tokens).parse()?;

        match statement {
            Statement::CreateTable(create) => self.create_table(
                create.name,
                Schema {
                    columns: create.columns,
                },
            )?,
            Statement::InsertInto(insert) => {
                self.insert(insert)?;
            }
            Statement::Delete(delete) => {
                self.delete(delete)?;
            }
            Statement::Update(update) => {
                self.update(update)?;
            }
            _ => {
                return Err(format!(
                    "Statement {:?} is not an executable statement",
                    statement
                ));
            }
        };
        Ok(())
    }

    /// Internal helper to handle row insertion logic.
    ///
    /// It maps provided values to the correct columns, handling cases where:
    /// 1. Columns are not specified (positional insertion).
    /// 2. Columns are specified in a different order than the schema.
    /// 3. Some columns are missing (filling them with `NULL`).
    fn insert(&mut self, insert: InsertInto) -> Result<(), String> {
        let table = self
            .get_table_mut(&insert.table)
            .ok_or_else(|| format!("Table {:?} does not exist", insert.table))?;

        let values = match insert.columns {
            None => insert.values,
            Some(columns) => {
                // Validate that all specified columns exist in the schema
                for col_name in &columns {
                    if !table.schema.columns.iter().any(|c| &c.name == col_name) {
                        return Err(format!(
                            "Column {:?} does not exist in table {:?}",
                            col_name, insert.table
                        ));
                    }
                }

                // Map provided values to their column names
                let mut provided_values: HashMap<String, Value> =
                    columns.into_iter().zip(insert.values).collect();

                // Build the final row by following the schema's column order
                table
                    .schema
                    .columns
                    .iter()
                    .map(|col| provided_values.remove(&col.name).unwrap_or(Value::Null))
                    .collect()
            }
        };

        table.insert(values)
    }

    /// A generic helper function to filter rows within a table.
    ///
    /// This method performs the following operations:
    /// 1. Pivots the data from column-oriented format to row-oriented format.
    /// 2. Evaluates the provided `WHERE` clause for every row.
    /// 3. Applies a mapping function (`map_fn`) to rows that satisfy the condition.
    ///
    /// # Arguments
    /// * `table` - A reference to the table to be scanned.
    /// * `where_clause` - An optional expression used to filter rows.
    /// * `map_fn` - A closure that determines what data to collect for each matching row
    ///   (e.g., the row's index or the row's values).
    fn filter_rows<T, F>(
        &self,
        table: &Table,
        where_clause: Option<&Expr>,
        mut map_fn: F,
    ) -> Result<Vec<T>, String>
    where
        F: FnMut(usize, &Vec<Value>) -> T,
    {
        let all_cols_data: Vec<Vec<Value>> = table
            .schema
            .columns
            .iter()
            .map(|col_def| {
                let column = table
                    .get_col(&col_def.name)
                    .ok_or_else(|| format!("column {:?} does not exist", col_def.name))?;

                let mut data = Vec::with_capacity(column.len());
                for i in 0..column.len() {
                    data.push(column.get(i).unwrap_or(Value::Null));
                }
                Ok(data)
            })
            .collect::<Result<Vec<_>, String>>()?;

        // Pivot: transform the data from Column-oriented (Vec of Columns)
        // to Row-oriented (Vec of Rows) for the final result.
        let row_count = all_cols_data.first().map(|c| c.len()).unwrap_or(0);
        let mut results = Vec::new();

        for i in 0..row_count {
            let full_row: Vec<Value> = all_cols_data.iter().map(|col| col[i].clone()).collect();

            let should_include = match where_clause {
                Some(expr) => self.evaluate_expr(expr, &full_row, &table.schema)?,
                None => true,
            };

            if should_include {
                // use closure to know what to store (index or row matching select clause)
                results.push(map_fn(i, &full_row));
            }
        }

        Ok(results)
    }

    /// Executes a `DELETE` statement.
    ///
    /// Deletion is performed in two phases:
    /// 1. Identification: It finds the indices of all rows matching the `WHERE` clause.
    /// 2. Removal: It removes those rows from the table storage.
    ///
    /// Note: Indices are sorted in descending order before deletion. This ensures that
    /// removing a row doesn't shift the positions of other rows that are still
    /// scheduled for deletion.
    ///
    /// # Errors
    /// Returns an error if the table is not found or if the `WHERE` clause contains
    /// invalid column names or type mismatches.
    fn delete(&mut self, delete: Delete) -> Result<(), String> {
        let rows_to_delete = {
            let table = self
                .get_table(&delete.table)
                .ok_or_else(|| format!("table {:?} does not exist", delete.table))?;

            self.filter_rows(table, Some(&delete.where_clause), |i, _| i)?
        };

        let table = self
            .get_table_mut(&delete.table)
            .ok_or_else(|| format!("table {:?} does not exist", delete.table))?;

        let mut rows = rows_to_delete;
        // use sort_unstable_by because rows indexes are unique and so we don't care about egality
        // order.
        rows.sort_unstable_by(|a, b| b.cmp(a));

        for index in rows {
            table.delete_row(index)?;
        }

        Ok(())
    }

    /// Executes an `UPDATE` statement to modify existing rows in a table.
    ///
    /// The update is performed in two phases:
    /// 1. **Identification**: It scans the table to find indices of rows matching the `WHERE` clause.
    /// 2. **Modification**: For every column assignment, it updates the values at the identified
    ///    indices in the columnar storage.
    ///
    /// # Errors
    /// Returns an error string if:
    /// - The target table does not exist.
    /// - One of the target columns does not exist.
    /// - The provided value's type does not match the column's data type.
    /// - The `WHERE` clause evaluation fails.
    fn update(&mut self, update: Update) -> Result<(), String> {
        let rows_to_update = {
            let table = self
                .get_table(&update.table)
                .ok_or_else(|| format!("table {:?} does not exist", update.table))?;
            self.filter_rows(table, Some(&update.where_clause), |i, _| i)?
        };
        let table = self
            .get_table_mut(&update.table)
            .ok_or_else(|| format!("table {:?} does not exist", update.table))?;
        let mut rows = rows_to_update;
        for (col, value) in update.assignments {
            let column = table
                .get_col_mut(&col)
                .ok_or_else(|| format!("column {:?} is not a column from this table", col))?;
            for row in &mut rows {
                column.set(*row, &value)?;
            }
        }
        Ok(())
    }

    /// Executes a `SELECT` query and returns the resulting data set.
    ///
    /// This method performs the full query lifecycle:
    /// 1. **Tokenizes** the SQL string.
    /// 2. **Parses** it into a `Select` AST node.
    /// 3. **Pivots** the columnar data into rows.
    /// 4. **Filters** rows based on the `WHERE` clause.
    /// 5. **Sorts** the remaining rows based on the `ORDER BY` clause.
    /// 6. **Projects** only the requested columns.
    /// 7. **Limits** the number of rows returned.
    ///
    /// # Example
    ///
    /// ```
    /// use db::{Database, Value};
    ///
    /// let mut db = Database::new();
    /// db.execute("CREATE TABLE products (name TEXT, price INT)").unwrap();
    /// db.execute("INSERT INTO products VALUES ('Laptop', 1200)").unwrap();
    /// db.execute("INSERT INTO products VALUES ('Mouse', 25)").unwrap();
    ///
    /// // Querying specific columns
    /// let result = db.query("SELECT name FROM products").unwrap();
    ///
    /// assert_eq!(result.columns, vec!["name"]);
    /// assert_eq!(result.rows.len(), 2);
    /// assert_eq!(result.rows[0][0], Value::Text("Laptop".into()));
    /// assert_eq!(result.rows[1][0], Value::Text("Mouse".into()));
    /// ```
    ///
    /// # Errors
    /// Returns an error string if:
    /// - The SQL is not a valid `SELECT` statement.
    /// - The specified table does not exist.
    /// - One or more specified columns do not exist.
    pub fn query(&self, sql: &str) -> Result<QueryResult, String> {
        let tokens = Tokenizer::new(sql).tokenize()?;
        let statement = Parser::new(tokens).parse()?;

        if !matches!(statement, Statement::Select(_)) {
            return Err(format!(
                "Statement {:?} is not a queryable statement",
                statement
            ));
        }

        let Statement::Select(select) = statement else {
            unreachable!()
        };

        let table = self
            .get_table(&select.table)
            .ok_or_else(|| format!("table {:?} does not exist", select.table))?;

        // Resolve which columns need to be projected
        let selected_cols = match select.columns {
            ColumnsSelect::Star => table
                .schema
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect(),
            ColumnsSelect::ColumnsNames(cols) => cols,
        };

        let mut filtered_rows =
            self.filter_rows(table, select.where_clause.as_ref(), |_, row| row.clone())?;

        if let Some(order_by) = select.order_by.filter(|o| !o.is_empty()) {
            self.sort(&mut filtered_rows, &table.schema.columns, order_by)?;
        }

        let final_rows: Vec<Vec<Value>> = filtered_rows
            .into_iter()
            .map(|full_row| {
                selected_cols
                    .iter()
                    .map(|col_name| {
                        let idx = table
                            .schema
                            .columns
                            .iter()
                            .position(|c| &c.name == col_name)
                            .unwrap();
                        // TODO: Use Rc or Arc to avoid clone
                        full_row[idx].clone()
                    })
                    .collect()
            })
            .take(select.limit.unwrap_or(usize::MAX))
            .collect();

        Ok(QueryResult {
            columns: selected_cols,
            rows: final_rows,
        })
    }

    /// Sorts the provided rows in-place based on the SQL `ORDER BY` clauses.
    ///
    /// This method supports multi-column sorting. For each row comparison, it
    /// iterates through the sort clauses: if the first column results in an
    /// equal comparison, it moves to the next column, and so on.
    ///
    /// # Errors
    /// Returns an error if a column specified in the `ORDER BY` clause
    /// does not exist in the table schema.
    fn sort(
        &self,
        rows: &mut [Vec<Value>],
        cols: &[ColumnDef],
        order_by: Vec<OrderByClause>,
    ) -> Result<(), String> {
        // save all column indexes on which we have to sort + boolean if we have to reverse order
        let sort = order_by
            .iter()
            .map(|clause| {
                let idx = cols
                    .iter()
                    .position(|c| c.name == clause.column)
                    .ok_or_else(|| format!("Column {} not found", clause.column))?;
                Ok((idx, clause.direction == SortDirection::Desc))
            })
            .collect::<Result<Vec<(usize, bool)>, String>>()?;

        rows.sort_by(|a, b| {
            for (idx, is_desc) in &sort {
                let mut ord = a[*idx].cmp(&b[*idx]);

                if *is_desc {
                    ord = ord.reverse();
                }
                // if it's not equal no need to compare more
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        Ok(())
    }

    /// Evaluates a WHERE clause expression against a specific row.
    ///
    /// This function recursively evaluates:
    /// - **Comparisons**: Column values compared to literals (`age > 18`)
    /// - **AND**: Logical conjunction with short-circuit evaluation
    /// - **OR**: Logical disjunction with short-circuit evaluation
    ///
    /// # Arguments
    /// * `expr` - The expression tree to evaluate
    /// * `row` - The complete row (all columns in schema order)
    /// * `schema` - Table schema used to resolve column indices
    ///
    /// # Returns
    /// * `Ok(true)` - The row satisfies the condition
    /// * `Ok(false)` - The row does not match
    /// * `Err(...)` - Invalid column name or type mismatch
    /// ```
    fn evaluate_expr(&self, expr: &Expr, row: &[Value], schema: &Schema) -> Result<bool, String> {
        match expr {
            Expr::Comparison { column, op, value } => {
                let col_idx = schema
                    .columns
                    .iter()
                    .position(|c| &c.name == column)
                    .ok_or_else(|| format!("Column {} not found", column))?;

                let row_value = &row[col_idx];

                self.compare_values(row_value, op, value)
            }
            Expr::Or { left, right } => {
                Ok(self.evaluate_expr(left, row, schema)?
                    || self.evaluate_expr(right, row, schema)?)
            }
            Expr::And { left, right } => {
                let left_result = self.evaluate_expr(left, row, schema)?;
                if !left_result {
                    return Ok(false);
                }
                self.evaluate_expr(right, row, schema)
            }
        }
    }

    /// Compares two values using a comparison operator.
    ///
    /// # SQL NULL Semantics
    /// - `NULL` compared to anything (including `NULL`) always returns `false`.
    /// - This matches standard SQL three-valued logic.
    ///
    /// # Supported Comparisons
    /// - **Integers**: `>`, `<`, `=`
    /// - **Floats**: `>`, `<`, `=` (with epsilon comparison)
    /// - **Text**: `=` (exact string match)
    /// - **Booleans**: `=`
    ///
    /// # Errors
    /// Returns an error if comparing incompatible types (e.g., `Int` vs `Text`).
    fn compare_values(
        &self,
        left: &Value,
        op: &ComparisonOp,
        right: &Value,
    ) -> Result<bool, String> {
        // NULL handling : NULL comparé à quoi que ce soit = false
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return Ok(false);
        }

        match (left, op, right) {
            // Int comparisons
            (Value::Int(l), ComparisonOp::Gt, Value::Int(r)) => Ok(l > r),
            (Value::Int(l), ComparisonOp::Lt, Value::Int(r)) => Ok(l < r),
            (Value::Int(l), ComparisonOp::Eq, Value::Int(r)) => Ok(l == r),

            // Float comparisons
            (Value::Float(l), ComparisonOp::Gt, Value::Float(r)) => Ok(l > r),
            (Value::Float(l), ComparisonOp::Lt, Value::Float(r)) => Ok(l < r),
            (Value::Float(l), ComparisonOp::Eq, Value::Float(r)) => {
                Ok((l - r).abs() < f64::EPSILON)
            }

            // Text comparisons
            (Value::Text(l), ComparisonOp::Eq, Value::Text(r)) => Ok(l == r),

            // Bool comparisons
            (Value::Bool(l), ComparisonOp::Eq, Value::Bool(r)) => Ok(l == r),

            // Type mismatch
            _ => Err(format!(
                "Type mismatch: cannot compare {:?} with {:?}",
                left, right
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::DataType;
    use crate::table::{ColumnDef, Schema};
    use crate::value::Value;

    fn simple_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                },
            ],
        }
    }

    #[test]
    fn test_create_and_drop_table() {
        let mut db = Database::new();

        assert!(
            db.create_table("users".to_string(), simple_schema())
                .is_ok()
        );
        assert!(db.get_table("users").is_some());

        assert!(db.drop_table("users").is_ok());
        assert!(db.get_table("users").is_none());
    }

    #[test]
    fn test_duplicate_table_error() {
        let mut db = Database::new();

        assert!(
            db.create_table("users".to_string(), simple_schema())
                .is_ok()
        );
        let err = db.create_table("users".to_string(), simple_schema());

        assert!(err.is_err());
    }

    #[test]
    fn test_drop_nonexistent_table() {
        let mut db = Database::new();

        let err = db.drop_table("unknown");
        assert!(err.is_err());
    }

    #[test]
    fn test_list_tables() {
        let mut db = Database::new();

        db.create_table("users".to_string(), simple_schema())
            .unwrap();
        db.create_table("posts".to_string(), simple_schema())
            .unwrap();

        let mut tables = db.list_tables();
        tables.sort();

        assert_eq!(tables, vec!["posts", "users"]);
    }

    #[test]
    fn test_get_table_mut() {
        let mut db = Database::new();
        db.create_table("users".to_string(), simple_schema())
            .unwrap();

        {
            let table = db.get_table_mut("users").unwrap();
            table
                .insert(vec![Value::Int(1), Value::Text("Alice".into())])
                .unwrap();
            table
                .insert(vec![Value::Int(2), Value::Text("Bob".into())])
                .unwrap();
        }

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count, 2);
        assert_eq!(
            table.get_row(0),
            Some(vec![Value::Int(1), Value::Text("Alice".into())])
        );
        assert_eq!(
            table.get_row(1),
            Some(vec![Value::Int(2), Value::Text("Bob".into())])
        );
    }

    #[test]
    fn test_execute_insert_and_query_star() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name TEXT)")
            .unwrap();

        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();

        let result = db.query("SELECT * FROM users").unwrap();

        assert_eq!(result.columns, vec!["id", "name"]);

        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0],
            vec![Value::Int(1), Value::Text("Alice".into())]
        );
        assert_eq!(
            result.rows[1],
            vec![Value::Int(2), Value::Text("Bob".into())]
        );
    }

    #[test]
    fn test_insert_with_column_reordering() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name TEXT)")
            .unwrap();

        db.execute("INSERT INTO users (name, id) VALUES ('Charlie', 3)")
            .unwrap();

        let result = db.query("SELECT id, name FROM users").unwrap();

        // Check first line, the order must be order of the schema
        assert_eq!(
            result.rows[0],
            vec![Value::Int(3), Value::Text("Charlie".into())]
        );
    }

    #[test]
    fn test_insert_partial_columns() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name TEXT)")
            .unwrap();

        db.execute("INSERT INTO users (id) VALUES (4)").unwrap();

        let result = db.query("SELECT name, id FROM users").unwrap();

        // Row must be [Null, 4] as we asked name before id in the query
        assert_eq!(result.rows[0], vec![Value::Null, Value::Int(4)]);
    }

    #[test]
    fn test_query_specific_columns_subset() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name TEXT, age INT)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();

        let result = db.query("SELECT name FROM users").unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows[0], vec![Value::Text("Alice".into())]);
    }

    #[test]
    fn test_query_with_where_simple() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name TEXT, age INT)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 17)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 25)")
            .unwrap();

        let result = db.query("SELECT name FROM users WHERE age > 18").unwrap();

        assert_eq!(result.columns, vec!["name"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0], vec![Value::Text("Alice".into())]);
        assert_eq!(result.rows[1], vec![Value::Text("Charlie".into())]);
    }

    #[test]
    fn test_query_with_where_and() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT, active INT)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 30, 1)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 17, 1)").unwrap();
        db.execute("INSERT INTO users VALUES (3, 25, 0)").unwrap();

        // WHERE age > 18 AND active = 1
        let result = db
            .query("SELECT id FROM users WHERE age > 18 AND active = 1")
            .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0], vec![Value::Int(1)]);
    }

    #[test]
    fn test_query_with_null_comparison() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT)").unwrap();
        db.execute("INSERT INTO users (id) VALUES (1)").unwrap(); // age = NULL
        db.execute("INSERT INTO users VALUES (2, 25)").unwrap();

        // NULL > 18 should be false (SQL semantics)
        let result = db.query("SELECT id FROM users WHERE age > 18").unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0], vec![Value::Int(2)]);
    }

    #[test]
    fn test_query_with_limit() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT, active INT)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 30, 1)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 17, 1)").unwrap();
        db.execute("INSERT INTO users VALUES (3, 25, 0)").unwrap();

        // WHERE age > 18 AND active = 1
        let result = db.query("SELECT id FROM users LIMIT 2").unwrap();

        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0], vec![Value::Int(1)]);
        assert_eq!(result.rows[1], vec![Value::Int(2)]);
    }

    #[test]
    fn test_query_with_where_no_match() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 15)").unwrap();

        let result = db.query("SELECT id FROM users WHERE age > 100").unwrap();

        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_query_order_by_asc_desc() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 30)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 20)").unwrap();
        db.execute("INSERT INTO users VALUES (3, 25)").unwrap();

        // Test ASC
        let res_asc = db.query("SELECT age FROM users ORDER BY age ASC").unwrap();
        assert_eq!(res_asc.rows[0][0], Value::Int(20));
        assert_eq!(res_asc.rows[2][0], Value::Int(30));

        // Test DESC
        let res_desc = db.query("SELECT age FROM users ORDER BY age DESC").unwrap();
        assert_eq!(res_desc.rows[0][0], Value::Int(30));
        assert_eq!(res_desc.rows[2][0], Value::Int(20));
    }

    #[test]
    fn test_query_order_by_multiple_columns() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (name TEXT, score INT)")
            .unwrap();
        db.execute("INSERT INTO users VALUES ('Alice', 100)")
            .unwrap();
        db.execute("INSERT INTO users VALUES ('Bob', 100)").unwrap();
        db.execute("INSERT INTO users VALUES ('Charlie', 50)")
            .unwrap();

        let res = db
            .query("SELECT name, score FROM users ORDER BY score DESC, name ASC")
            .unwrap();

        assert_eq!(res.rows[0][0], Value::Text("Alice".into()));
        assert_eq!(res.rows[1][0], Value::Text("Bob".into()));
        assert_eq!(res.rows[2][0], Value::Text("Charlie".into()));
    }

    #[test]
    fn test_query_order_by_hidden_column() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 30)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 20)").unwrap();

        let res = db.query("SELECT id FROM users ORDER BY age ASC").unwrap();

        assert_eq!(res.columns, vec!["id"]);
        assert_eq!(res.rows[0][0], Value::Int(2)); // age 20
        assert_eq!(res.rows[1][0], Value::Int(1)); // age 30
    }

    #[test]
    fn test_query_order_by_with_limit() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT)").unwrap();
        for i in 1..=10 {
            db.execute(&format!("INSERT INTO users VALUES ({})", i))
                .unwrap();
        }

        let res = db
            .query("SELECT id FROM users ORDER BY id DESC LIMIT 3")
            .unwrap();

        assert_eq!(res.rows.len(), 3);
        assert_eq!(res.rows[0][0], Value::Int(10));
        assert_eq!(res.rows[2][0], Value::Int(8));
    }

    #[test]
    fn test_delete_specific_row() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();

        // Delete Alice
        db.execute("DELETE FROM users WHERE id = 1").unwrap();

        let result = db.query("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Int(2)); // Only Bob remains
        assert_eq!(result.rows[0][1], Value::Text("Bob".into()));
    }

    #[test]
    fn test_delete_multiple_rows() {
        let mut db = Database::new();
        db.execute("CREATE TABLE products (id INT, price INT)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO products VALUES (2, 50)").unwrap();
        db.execute("INSERT INTO products VALUES (3, 100)").unwrap();
        db.execute("INSERT INTO products VALUES (4, 20)").unwrap();

        // Delete all expensive products (> 40)
        db.execute("DELETE FROM products WHERE price > 40").unwrap();

        let result = db.query("SELECT id FROM products ORDER BY id ASC").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Value::Int(1));
        assert_eq!(result.rows[1][0], Value::Int(4));
    }

    #[test]
    fn test_delete_no_match() {
        let mut db = Database::new();
        db.create_table("users".to_string(), simple_schema())
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();

        // Try to delete a non-matching row
        db.execute("DELETE FROM users WHERE id = 99").unwrap();

        let result = db.query("SELECT id FROM users").unwrap();
        assert_eq!(result.rows.len(), 1); // The row is still there
    }

    #[test]
    fn test_delete_with_complex_condition() {
        let mut db = Database::new();
        db.execute("CREATE TABLE tasks (id INT, priority INT, done INT)")
            .unwrap();
        db.execute("INSERT INTO tasks VALUES (1, 1, 1)").unwrap(); // High priority, finished
        db.execute("INSERT INTO tasks VALUES (2, 1, 0)").unwrap(); // High priority, not finished
        db.execute("INSERT INTO tasks VALUES (3, 5, 1)").unwrap(); // Low priority, finished

        // Delete tasks that are either low priority OR already finished
        db.execute("DELETE FROM tasks WHERE priority > 3 OR done = 1")
            .unwrap();

        let result = db.query("SELECT id FROM tasks").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::Int(2)); // Only task 2 remains
    }

    #[test]
    fn test_delete_all_rows_condition() {
        let mut db = Database::new();
        db.execute("CREATE TABLE data (id INT)").unwrap();
        db.execute("INSERT INTO data VALUES (1)").unwrap();
        db.execute("INSERT INTO data VALUES (2)").unwrap();

        // Use a condition that is always true to empty the table
        db.execute("DELETE FROM data WHERE id > 0").unwrap();

        let result = db.query("SELECT * FROM data").unwrap();
        assert_eq!(result.rows.len(), 0);
    }

    #[test]
    fn test_update_single_column() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 30)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 20)").unwrap();

        db.execute("UPDATE users SET age = 99 WHERE id = 1")
            .unwrap();

        let result = db.query("SELECT age FROM users ORDER BY id ASC").unwrap();
        assert_eq!(result.rows[0][0], Value::Int(99)); // id=1, age modifié
        assert_eq!(result.rows[1][0], Value::Int(20)); // id=2, inchangé
    }
    #[test]
    fn test_update_multiple_columns() {
        let mut db = Database::new();
        db.execute("CREATE TABLE products (id INT, name TEXT, price INT)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (1, 'Keyboard', 50)")
            .unwrap();

        // Update two columns simultaneously
        db.execute("UPDATE products SET name = 'Mechanical Keyboard', price = 120 WHERE id = 1")
            .unwrap();

        let result = db.query("SELECT name, price FROM products").unwrap();
        assert_eq!(result.rows[0][0], Value::Text("Mechanical Keyboard".into()));
        assert_eq!(result.rows[0][1], Value::Int(120));
    }

    #[test]
    fn test_update_with_complex_where() {
        let mut db = Database::new();
        db.execute("CREATE TABLE employees (id INT, dept TEXT, salary INT)")
            .unwrap();
        db.execute("INSERT INTO employees VALUES (1, 'IT', 3000)")
            .unwrap();
        db.execute("INSERT INTO employees VALUES (2, 'HR', 2500)")
            .unwrap();
        db.execute("INSERT INTO employees VALUES (3, 'IT', 3500)")
            .unwrap();

        db.execute("UPDATE employees SET salary = 3300 WHERE dept = 'IT' AND salary < 3200")
            .unwrap();

        let result = db
            .query("SELECT id, salary FROM employees ORDER BY id ASC")
            .unwrap();
        assert_eq!(result.rows[0][1], Value::Int(3300));
        assert_eq!(result.rows[1][1], Value::Int(2500));
        assert_eq!(result.rows[2][1], Value::Int(3500));
    }

    #[test]
    fn test_update_type_mismatch_error() {
        let mut db = Database::new();
        db.execute("CREATE TABLE test (id INT)").unwrap();
        db.execute("INSERT INTO test VALUES (1)").unwrap();

        let result = db.execute("UPDATE test SET id = 'Invalid' WHERE id = 1");

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("type"));
    }

    #[test]
    fn test_update_no_rows_matched() {
        let mut db = Database::new();
        db.execute("CREATE TABLE test (id INT, val INT)").unwrap();
        db.execute("INSERT INTO test VALUES (1, 10)").unwrap();

        db.execute("UPDATE test SET val = 99 WHERE id = 404")
            .unwrap();

        let result = db.query("SELECT val FROM test").unwrap();
        assert_eq!(result.rows[0][0], Value::Int(10));
    }

    #[test]
    fn test_update_non_existent_column() {
        let mut db = Database::new();
        db.execute("CREATE TABLE test (id INT)").unwrap();
        db.execute("INSERT INTO test VALUES (1)").unwrap();

        let result = db.execute("UPDATE test SET unknown_col = 10 WHERE id = 1");
        assert!(result.is_err());
    }
}
