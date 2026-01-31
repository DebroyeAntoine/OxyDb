use crate::column::Column;
use crate::data_type::DataType;
use crate::value::Value;

/// Represents the definition of a single column in a table's schema.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// The name of the column.
    pub name: String,
    /// The data type allowed for this column.
    pub data_type: DataType,
}

/// Defines the structure of a table, consisting of an ordered list of column definitions.
#[derive(Debug, Clone)]
pub struct Schema {
    /// The ordered collection of column definitions.
    pub columns: Vec<ColumnDef>,
}

/// Represents a database table stored in memory.
///
/// Data is stored in a columnar format (one [Column] per schema field) to improve
/// memory locality and performance for analytical queries.
pub struct Table {
    /// The unique name of the table.
    pub name: String,
    /// The structural definition of the table.
    pub schema: Schema,
    /// The actual data storage, where each [Column] matches a [ColumnDef] in the schema.
    pub columns: Vec<Column>,
    /// The total number of rows currently stored in the table.
    pub row_count: usize,
}

impl Table {
    /// Creates a new table with the given name and schema.
    ///
    /// This initializes the underlying [Column] storage for each field defined in the schema.
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

    /// Appends a new row of values to the table.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The number of values provided does not match the number of columns in the schema.
    /// - The data type of any value does not match the corresponding column's data type.
    pub fn insert(&mut self, values: Vec<Value>) -> Result<(), String> {
        // Validate row length
        if values.len() != self.schema.columns.len() {
            return Err(format!(
                "size of the row mismatched with the size of a table row, {:?} vs {:?}",
                values.len(),
                self.schema.columns.len()
            ));
        }

        // Validate types and push values to respective columns
        for (i, value) in values.into_iter().enumerate() {
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

    /// Retrieves a full row of values at the specified index.
    ///
    /// Since the database is columnar, this method reconstructs the row by
    /// fetching the value at `row_idx` from every column.
    ///
    /// Returns `None` if the index is out of bounds.
    pub fn get_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        if self.row_count <= row_idx {
            return None;
        }
        self.columns.iter().map(|col| col.get(row_idx)).collect() // Reconstructs the row as Vec<Value>
    }

    /// Finds and returns a reference to a specific column by its name.
    ///
    /// Returns `None` if no column with the given name exists in this table.
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
