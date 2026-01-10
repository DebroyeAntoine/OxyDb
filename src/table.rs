use crate::column::Column;
use crate::data_type::DataType;
use crate::value::Value;

/// Column definition in the schema
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

pub struct Table {
    pub name: String,
    pub schema: Schema,
    pub columns: Vec<Column>,
    pub row_count: usize,
}

impl Table {
    pub fn new(name: String, schema: Schema) -> Self {
        let columns = schema
            .columns
            .iter()
            .map(|column| Column::new(column.name.clone(), column.data_type))
            .collect();
        Self {
            name,
            schema,
            columns,
            row_count: 0,
        }
    }

    /// insert a new row
    pub fn insert(&mut self, values: Vec<Value>) -> Result<(), String> {
        // different sizes
        if values.len() != self.schema.columns.len() {
            return Err(format!(
                "size of the row mismatched with the size of a table row, {:?} vs {:?}",
                values.len(),
                self.schema.columns.len()
            ));
        }
        for (i, value) in values.into_iter().enumerate() {
            // different types
            if value
                .data_type()
                .is_some_and(|t| t != self.schema.columns[i].data_type)
            {
                return Err(format!(
                    "type of value {:?} mismatch with schema column type: {:?} vs {:?}",
                    value,
                    value.data_type(),
                    self.schema.columns[i].data_type
                ));
            }
            self.columns[i].push(value)?;
        }
        self.row_count += 1;
        Ok(())
    }

    pub fn get_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        if self.row_count <= row_idx {
            return None;
        }
        self.columns
            .iter()
            .map(|col| col.get(row_idx)) // -> Option<Value>
            .collect()
    }

    pub fn get_col(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|col| col.name == name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_creation() {
        let schema = Schema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "name".into(),
                    data_type: DataType::Text,
                },
            ],
        };

        let table = Table::new("users".into(), schema);
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.row_count, 0);
    }

    #[test]
    fn test_table_insert_and_get() {
        let schema = Schema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "age".into(),
                    data_type: DataType::Int,
                },
            ],
        };

        let mut table = Table::new("test".into(), schema);

        table.insert(vec![Value::Int(1), Value::Int(30)]).unwrap();
        table.insert(vec![Value::Int(2), Value::Null]).unwrap();

        assert_eq!(table.row_count, 2);

        let row0 = table.get_row(0).unwrap();
        assert_eq!(row0, vec![Value::Int(1), Value::Int(30)]);

        let row1 = table.get_row(1).unwrap();
        assert_eq!(row1, vec![Value::Int(2), Value::Null]);
    }

    #[test]
    fn test_column_count_mismatch() {
        let schema = Schema {
            columns: vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
            }],
        };

        let mut table = Table::new("test".into(), schema);

        // Trop de colonnes
        let result = table.insert(vec![Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());

        // Pas assez de colonnes
        let result = table.insert(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_type_mismatch_error() {
        let schema = Schema {
            columns: vec![ColumnDef {
                name: "id".into(),
                data_type: DataType::Int,
            }],
        };

        let mut table = Table::new("test".into(), schema);

        // Type incorrect
        let result = table.insert(vec![Value::Text("hello".into())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef {
                    name: "id".into(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "name".into(),
                    data_type: DataType::Text,
                },
            ],
        };

        let table = Table::new("users".into(), schema);

        assert!(table.get_col("id").is_some());
        assert!(table.get_col("name").is_some());
        assert!(table.get_col("age").is_none());
    }
}
