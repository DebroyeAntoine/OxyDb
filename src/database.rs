use crate::{
    Value,
    ast::{ColumnsSelect, ComparisonOp, Expr, InsertInto, Statement},
    parser::Parser,
    table::{Schema, Table},
    tokenizer::Tokenizer,
};
use std::collections::HashMap;

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

    /// Executes a `SELECT` query and returns the resulting data set.
    ///
    /// This method performs the full query lifecycle:
    /// 1. **Tokenizes** the SQL string.
    /// 2. **Parses** it into a `Select` AST node.
    /// 3. **Projects** the requested columns from the columnar storage.
    /// 4. **Pivots** the data from columns back into rows for the result.
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
        let row_count = if all_cols_data.is_empty() {
            0
        } else {
            all_cols_data[0].len()
        };

        let mut filtered_rows = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let mut full_row = Vec::with_capacity(all_cols_data.len());
            for col in &all_cols_data {
                full_row.push(col[i].clone());
            }

            let should_include = match &select.where_clause {
                Some(where_expr) => self.evaluate_expr(where_expr, &full_row, &table.schema)?,
                None => true,
            };

            if should_include {
                filtered_rows.push(full_row);
            }
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
                        full_row[idx].clone()
                    })
                    .collect()
            })
            .collect();

        Ok(QueryResult {
            columns: selected_cols,
            rows: final_rows,
        })
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

                // 2. Récupérer la valeur dans la row
                let row_value = &row[col_idx];

                // 3. Comparer (la vérification de type est DANS compare_values)
                self.compare_values(row_value, op, value)
            }
            Expr::Or { left, right } => {
                Ok(self.evaluate_expr(left, row, schema)?
                    || self.evaluate_expr(right, row, schema)?)
            }
            Expr::And { left, right } => {
                let left_result = self.evaluate_expr(left, row, schema)?;
                if !left_result {
                    return Ok(false); // Short-circuit !
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
    fn test_query_with_where_no_match() {
        let mut db = Database::new();
        db.execute("CREATE TABLE users (id INT, age INT)").unwrap();
        db.execute("INSERT INTO users VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 15)").unwrap();

        let result = db.query("SELECT id FROM users WHERE age > 100").unwrap();

        assert_eq!(result.rows.len(), 0); // Aucune row ne match
    }
}
