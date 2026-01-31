use crate::{
    Value,
    ast::{ColumnsSelect, InsertInto, Statement},
    parser::Parser,
    table::{Schema, Table},
    tokenizer::Tokenizer,
};
use std::collections::HashMap;

#[derive(Default)]
pub struct Database {
    tables: HashMap<String, Table>,
}

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::default(),
        }
    }

    pub fn create_table(&mut self, name: String, schema: Schema) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table {} already exists in the database", name));
        }
        let table = Table::new(name.clone(), schema); // maybe use Arc later to avoid this clone
        self.tables.insert(name, table);
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), String> {
        match self.tables.remove(name) {
            Some(_) => Ok(()),
            None => Err(format!("Table {} does not exist in the database", name)),
        }
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn list_tables(&self) -> Vec<&str> {
        self.tables.iter().map(|m| m.0.as_str()).collect()
    }

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

    fn insert(&mut self, insert: InsertInto) -> Result<(), String> {
        let table = self
            .get_table_mut(&insert.table)
            .ok_or_else(|| format!("Table {:?} does not exist", insert.table))?;

        let values = match insert.columns {
            None => insert.values,
            Some(columns) => {
                // Check there is no column absent in the schema
                for col_name in &columns {
                    if !table.schema.columns.iter().any(|c| &c.name == col_name) {
                        return Err(format!(
                            "Column {:?} does not exist in table {:?}",
                            col_name, insert.table
                        ));
                    }
                }
                // create hashmap by zipping columns and moving cols and values by using
                // into_iter()
                let mut provided_values: HashMap<String, Value> =
                    columns.into_iter().zip(insert.values).collect();

                // row to be inserted with the same order as the schema
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

        let selected_cols = match select.columns {
            ColumnsSelect::Star => table
                .schema
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect(),
            ColumnsSelect::ColumnsNames(cols) => cols,
        };

        let cols_data: Vec<Vec<Value>> = selected_cols
            .iter()
            .map(|col| {
                let column_table = table
                    .get_col(col)
                    .ok_or_else(|| format!("column {:?} does not exist", col))?;
                let mut data = Vec::with_capacity(column_table.len());
                for d in 0..column_table.len() {
                    data.push(column_table.get(d).unwrap_or(Value::Null))
                }
                Ok(data)
            })
            .collect::<Result<Vec<_>, String>>()?;

        // transform colmuns vec to row vec
        let row_count = if cols_data.is_empty() {
            0
        } else {
            cols_data[0].len()
        };
        let mut final_rows = Vec::with_capacity(row_count);

        for i in 0..row_count {
            let mut row = Vec::with_capacity(cols_data.len());
            for col in &cols_data {
                row.push(col[i].clone());
            }
            final_rows.push(row);
        }

        Ok(QueryResult {
            columns: selected_cols,
            rows: final_rows,
        })
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
}
