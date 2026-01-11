use crate::table::{Schema, Table};
use std::collections::HashMap;

pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
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

        assert!(db
            .create_table("users".to_string(), simple_schema())
            .is_ok());
        assert!(db.get_table("users").is_some());

        assert!(db.drop_table("users").is_ok());
        assert!(db.get_table("users").is_none());
    }

    #[test]
    fn test_duplicate_table_error() {
        let mut db = Database::new();

        assert!(db
            .create_table("users".to_string(), simple_schema())
            .is_ok());
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
}
