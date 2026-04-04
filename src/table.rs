use allocative::Allocative;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bitvec::prelude::*;

use crate::column::Column;
use crate::data_type::DataType;
use crate::database::VacuumConfig;
use crate::value::Value;

/// Represents the definition of a single column in a table's schema.
#[derive(Debug, Clone, PartialEq, Allocative)]
pub struct ColumnDef {
    /// The name of the column.
    pub name: String,
    /// The data type allowed for this column.
    pub data_type: DataType,
    /// If the value can be auto incremented WARNING: Only for Value::Int
    pub auto_increment: bool,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            auto_increment: false,
        }
    }

    /// Marks this column as auto-incremented. Only valid for `DataType::Int` columns.
    ///
    /// Uses the builder pattern so it can be chained after [`ColumnDef::new`]:
    /// `ColumnDef::new("id", DataType::Int).auto_increment()`
    ///
    /// When a table is created with such a column, `Table` will automatically
    /// assign a monotonically increasing `i64` to any row inserted with
    /// `Value::Null` at this position.
    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }
}

/// Defines the structure of a table, consisting of an ordered list of column definitions.
#[derive(Debug, Clone, Allocative)]
pub struct Schema {
    /// The ordered collection of column definitions.
    pub columns: Vec<ColumnDef>,
}

impl Schema {
    pub fn index_of(&self, name: &str) -> Result<usize, String> {
        self.columns
            .iter()
            .position(|col| col.name == name)
            .ok_or_else(|| format!("Column '{}' not found", name))
    }
}

/// Represents a database table stored in memory.
///
/// Data is stored in a columnar format (one [Column] per schema field) to improve
/// memory locality and performance for analytical queries.
#[derive(Debug, Allocative)]
pub struct Table {
    /// The unique name of the table.
    pub name: String,
    /// The structural definition of the table.
    pub schema: Schema,
    /// The actual data storage, where each [Column] matches a [ColumnDef] in the schema.
    pub columns: Vec<Column>,
    /// The total number of rows currently stored in the table.
    pub row_count: usize,
    /// The deletion vector.
    ///
    /// As we don't want to delete each rows one by one (too big complexcity when arranging
    /// vectors), we mark in this vector which line have to be deleted, and then the vacuum will
    /// delete them.
    #[allocative(skip)]
    pub deletion_vector: BitVec,
    /// String interner to use performance from Arc.
    ///
    /// If a string is used in the table, its strong count is as least 2 (one in the hashset and
    /// one in the table).
    string_interner: HashSet<Arc<str>>,

    /// Next value to assign when inserting a row with `Value::Null` in the
    /// auto-increment column. `None` if no column is auto-incremented.
    /// Advances on each insert and is recalculated by [`Table::vacuum`] to
    /// `max(remaining values) + 1`.
    pub next_auto_id: Option<i64>,
    /// Schema index (as `i64`) of the auto-increment column, or `None` if the
    /// table has no auto-increment column. Set once at table creation and never
    /// modified afterwards.
    pub idx_col_auto_id: Option<i64>,
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

        let idx_col_auto_id = schema
            .columns
            .iter()
            .position(|c| c.auto_increment)
            .map(|i| i as i64);
        let next_auto_id = idx_col_auto_id.map(|_| 1i64);

        Self {
            name,
            schema,
            columns,
            row_count: 0,
            deletion_vector: bitvec!(),
            string_interner: HashSet::default(),
            next_auto_id,
            idx_col_auto_id,
        }
    }

    fn internalize_string(&mut self, s: &mut Arc<str>) {
        if let Some(existing_arc) = self.string_interner.get(s) {
            *s = Arc::clone(existing_arc);
        } else {
            self.string_interner.insert(Arc::clone(s));
        }
    }

    /// Appends a new row of values to the table.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The number of values provided does not match the number of columns in the schema.
    /// - The data type of any value does not match the corresponding column's data type.
    pub fn insert(&mut self, mut values: Vec<Value>) -> Result<(), String> {
        // Validate row length
        if values.len() != self.schema.columns.len() {
            return Err(format!(
                "size of the row mismatched with the size of a table row, {:?} vs {:?}",
                values.len(),
                self.schema.columns.len()
            ));
        }

        // Fill auto-increment column if the value is Null
        if let (Some(idx), Some(next_id)) = (self.idx_col_auto_id, &mut self.next_auto_id) {
            let idx = idx as usize;
            if values[idx] == Value::Null {
                values[idx] = Value::Int(*next_id);
                *next_id += 1;
            } else if let Value::Int(v) = values[idx] {
                // Explicit value provided — keep counter ahead
                if v >= *next_id {
                    *next_id = v + 1;
                }
            }
        }

        // Validate types and push values to respective columns
        for (i, mut value) in values.into_iter().enumerate() {
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
            // if the value is a string check in the hashset if it already exists or not and
            // increase strong count if yes.
            if let Value::Text(ref mut s) = value {
                self.internalize_string(s);
            }
            self.columns[i].push(value)?;
        }

        self.deletion_vector.push(false);
        self.row_count += 1;
        Ok(())
    }

    /// Retrieves a full row of values at the specified index.
    ///
    /// Since the database is columnar, this method reconstructs the row by
    /// fetching the value at `row_idx` from every column.
    ///
    /// Returns `None` if the index is out of bounds or marked as deleted.
    pub fn get_row(&self, row_idx: usize) -> Option<Vec<Value>> {
        if self.row_count <= row_idx || self.deletion_vector[row_idx] {
            return None;
        }
        self.columns.iter().map(|col| col.get(row_idx)).collect() // Reconstructs the row as Vec<Value>
    }

    /// Delete a full row at the specific index.
    ///
    /// Returns a result if the remove as failed to catch the error.
    pub fn delete_row(&mut self, row_idx: usize) -> Result<(), String> {
        self.deletion_vector.replace(row_idx, true);
        Ok(())
    }

    /// Finds and returns a reference to a specific column by its name.
    ///
    /// Returns `None` if no column with the given name exists in this table.
    pub fn get_col(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|col| col.name == name)
    }

    /// Finds and returns a mutable reference to a specific column by its name.
    ///
    /// Returns `None` if no column with the given name exists in this table.
    fn get_col_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.columns.iter_mut().find(|col| col.name == name)
    }

    pub fn vacuum(&mut self) -> Result<(), String> {
        if !self.deletion_vector.any() {
            return Ok(());
        }

        let new_row_count = self.deletion_vector.count_zeros();

        for column in &mut self.columns {
            column.compact(&self.deletion_vector)?;
        }

        self.row_count = new_row_count;

        self.deletion_vector = BitVec::repeat(false, new_row_count);

        // Recalculate auto-increment counter from the remaining data
        if let Some(idx) = self.idx_col_auto_id {
            let col = &self.columns[idx as usize];
            if let crate::column::ColumnData::Int(ref ints) = col.data {
                let max_id = ints
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !col.null_bitmap[*i])
                    .map(|(_, v)| *v)
                    .max();
                self.next_auto_id = Some(max_id.map_or(1, |m| m + 1));
            }
        }

        // clean all Arc<str> with strong count to one because they are no more used in the table.
        self.string_interner
            .retain(|value| Arc::strong_count(value) > 1);
        Ok(())
    }

    /// Do an update of values at each row_idx wanted.
    pub fn update(
        &mut self,
        rows_idx: &[usize],
        values: HashMap<String, Value>,
    ) -> Result<(), String> {
        for (col, mut value) in values {
            if let Value::Text(ref mut s) = value {
                self.internalize_string(s);
            }
            let column = self
                .get_col_mut(&col)
                .ok_or_else(|| format!("column {:?} is not a column from this table", col))?;
            for row in rows_idx {
                column.set(*row, &value)?;
            }
        }
        Ok(())
    }

    /// Determines if the table needs a vacuum based on the provided configuration.
    pub fn should_vacuum(&self, config: &VacuumConfig) -> bool {
        if !config.enabled {
            return false;
        }

        let deleted = self.deletion_vector.count_ones();
        if deleted == 0 {
            return false;
        }

        // Even if we have many deleted rows, we only vacuum if they represent
        // a significant portion of the total table size.
        if self.row_count > 0 {
            let ratio = deleted as f64 / self.row_count as f64;
            return ratio >= config.deleted_ratio;
        }

        false
    }

    /// Check if a string is already in the string_interner and get its Arc.
    pub fn lookup_string(&self, s: &str) -> Option<Arc<str>> {
        self.string_interner.get(s).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_creation() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", DataType::Int),
                ColumnDef::new("name", DataType::Text),
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
                ColumnDef::new("id", DataType::Int),
                ColumnDef::new("age", DataType::Int),
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
            columns: vec![ColumnDef::new("id", DataType::Int)],
        };

        let mut table = Table::new("test".into(), schema);

        let result = table.insert(vec![Value::Int(1), Value::Int(2)]);
        assert!(result.is_err());

        let result = table.insert(vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn test_type_mismatch_error() {
        let schema = Schema {
            columns: vec![ColumnDef::new("id", DataType::Int)],
        };

        let mut table = Table::new("test".into(), schema);

        let result = table.insert(vec![Value::Text("hello".into())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", DataType::Int),
                ColumnDef::new("name", DataType::Text),
            ],
        };

        let table = Table::new("users".into(), schema);

        assert!(table.get_col("id").is_some());
        assert!(table.get_col("name").is_some());
        assert!(table.get_col("age").is_none());
    }

    #[test]
    fn test_remove_row() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", DataType::Int),
                ColumnDef::new("age", DataType::Int),
            ],
        };

        let mut table = Table::new("users".into(), schema);

        table.insert(vec![Value::Int(1), Value::Int(30)]).unwrap();
        table.insert(vec![Value::Int(2), Value::Null]).unwrap();

        assert_eq!(table.row_count, 2);

        table.delete_row(0).unwrap();

        assert_eq!(table.row_count, 2); // due to deletion_vector
        assert_eq!(table.get_row(0), None);
        assert!(table.deletion_vector[0]);
        let row1 = table.get_row(1).unwrap();
        assert_eq!(row1, vec![Value::Int(2), Value::Null]);
    }

    #[test]
    fn test_table_vacuum() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", DataType::Int),
                ColumnDef::new("val", DataType::Int),
            ],
        };
        let mut table = Table::new("test".into(), schema);

        table.insert(vec![Value::Int(1), Value::Int(10)]).unwrap();
        table.insert(vec![Value::Int(2), Value::Int(20)]).unwrap();
        table.insert(vec![Value::Int(3), Value::Int(30)]).unwrap();
        table.insert(vec![Value::Int(4), Value::Int(40)]).unwrap();

        table.delete_row(1).unwrap();
        table.delete_row(3).unwrap();

        assert_eq!(table.row_count, 4);
        assert!(table.deletion_vector[1]);
        println!("{}", table.deletion_vector);

        table.vacuum().unwrap();

        assert_eq!(table.row_count, 2);
        assert_eq!(table.deletion_vector.len(), 2);
        assert!(!table.deletion_vector.any());

        let row0 = table.get_row(0).unwrap();
        assert_eq!(row0[0], Value::Int(1));

        let row1 = table.get_row(1).unwrap();
        assert_eq!(row1[0], Value::Int(3));

        assert!(table.get_row(2).is_none());
    }

    fn make_auto_table() -> Table {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", DataType::Int).auto_increment(),
                ColumnDef::new("name", DataType::Text),
            ],
        };
        Table::new("users".into(), schema)
    }

    #[test]
    fn test_auto_increment_init() {
        let table = make_auto_table();
        assert_eq!(table.idx_col_auto_id, Some(0));
        assert_eq!(table.next_auto_id, Some(1));
    }

    #[test]
    fn test_auto_increment_fills_null() {
        let mut table = make_auto_table();
        table
            .insert(vec![Value::Null, Value::Text("Alice".into())])
            .unwrap();
        table
            .insert(vec![Value::Null, Value::Text("Bob".into())])
            .unwrap();

        assert_eq!(table.get_row(0).unwrap()[0], Value::Int(1));
        assert_eq!(table.get_row(1).unwrap()[0], Value::Int(2));
        assert_eq!(table.next_auto_id, Some(3));
    }

    #[test]
    fn test_auto_increment_explicit_value_advances_counter() {
        let mut table = make_auto_table();
        table
            .insert(vec![Value::Int(10), Value::Text("Alice".into())])
            .unwrap();
        table
            .insert(vec![Value::Null, Value::Text("Bob".into())])
            .unwrap();

        assert_eq!(table.get_row(0).unwrap()[0], Value::Int(10));
        assert_eq!(table.get_row(1).unwrap()[0], Value::Int(11));
        assert_eq!(table.next_auto_id, Some(12));
    }

    #[test]
    fn test_auto_increment_explicit_low_value_no_regression() {
        let mut table = make_auto_table();
        table
            .insert(vec![Value::Null, Value::Text("A".into())])
            .unwrap(); // id=1
        table
            .insert(vec![Value::Int(0), Value::Text("B".into())])
            .unwrap(); // explicit 0, counter stays at 2
        table
            .insert(vec![Value::Null, Value::Text("C".into())])
            .unwrap(); // id=2

        assert_eq!(table.get_row(1).unwrap()[0], Value::Int(0));
        assert_eq!(table.get_row(2).unwrap()[0], Value::Int(2));
    }

    #[test]
    fn test_auto_increment_vacuum_recalculates_counter() {
        let mut table = make_auto_table();
        table
            .insert(vec![Value::Null, Value::Text("A".into())])
            .unwrap(); // id=1
        table
            .insert(vec![Value::Null, Value::Text("B".into())])
            .unwrap(); // id=2
        table
            .insert(vec![Value::Null, Value::Text("C".into())])
            .unwrap(); // id=3

        // Delete the row with the highest id
        table.delete_row(2).unwrap();
        table.vacuum().unwrap();

        // Counter should be recalculated to max(1,2) + 1 = 3
        assert_eq!(table.next_auto_id, Some(3));

        table
            .insert(vec![Value::Null, Value::Text("D".into())])
            .unwrap();
        assert_eq!(table.get_row(2).unwrap()[0], Value::Int(3));
    }

    #[test]
    fn test_auto_increment_vacuum_all_deleted() {
        let mut table = make_auto_table();
        table
            .insert(vec![Value::Null, Value::Text("A".into())])
            .unwrap();
        table.delete_row(0).unwrap();
        table.vacuum().unwrap();

        // All rows gone → counter resets to 1
        assert_eq!(table.next_auto_id, Some(1));

        table
            .insert(vec![Value::Null, Value::Text("B".into())])
            .unwrap();
        assert_eq!(table.get_row(0).unwrap()[0], Value::Int(1));
    }

    #[test]
    fn test_no_auto_increment_table_unchanged() {
        let schema = Schema {
            columns: vec![ColumnDef::new("id", DataType::Int)],
        };
        let table = Table::new("t".into(), schema);
        assert_eq!(table.idx_col_auto_id, None);
        assert_eq!(table.next_auto_id, None);
    }
}
