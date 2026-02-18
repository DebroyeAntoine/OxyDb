use std::sync::Arc;

use crate::data_type::DataType;
use crate::value::Value;
use bitvec::prelude::*;

/// Physical storage for column data.
/// Each variant wraps a collection of a specific type to ensure contiguous memory
/// allocation (columnar storage).
#[derive(Debug, Clone)]
pub enum ColumnData {
    /// Vector of 64-bit integers.
    Int(Vec<i64>),
    /// Vector of 64-bit floats.
    Float(Vec<f64>),
    /// Vector of thread-safe atomic reference-counted strings.
    Text(Vec<Arc<str>>),
    /// Compact bit-vector for boolean values.
    Bool(BitVec),
}

/// Represents a column within a table.
/// It combines metadata (name, type) with actual data and a nullability tracker.
#[derive(Debug, Clone)]
pub struct Column {
    /// The name of the column.
    pub name: String,
    /// The logical data type of the column.
    pub data_type: DataType,
    /// The actual values stored in the column.
    pub data: ColumnData,
    /// A bitmap where a `true` bit indicates that the value at that index is `NULL`.
    pub null_bitmap: BitVec,
}

impl Column {
    /// Creates a new, empty column with the specified name and data type.
    /// The underlying data storage is initialized according to the data type.
    pub fn new(name: String, data_type: DataType) -> Self {
        let data = match data_type {
            DataType::Int => ColumnData::Int(vec![]),
            DataType::Float => ColumnData::Float(vec![]),
            DataType::Bool => ColumnData::Bool(bitvec!()),
            DataType::Text => ColumnData::Text(vec![]),
        };
        Self {
            name,
            data_type,
            data,
            null_bitmap: bitvec!(),
        }
    }

    /// Appends a new value to the end of the column.
    ///
    /// # Errors
    /// Returns an error if the value's type does not match the column's data type.
    ///
    /// # Behavior
    /// - If the value is `Null`, a default "dummy" value is pushed to the data vector
    ///   to maintain index alignment with the `null_bitmap`.
    /// - If the value is not `Null`, it is added to the data vector and the bitmap is updated.
    ///
    /// # Example
    /// ```
    /// # use db::column::Column;
    /// # use db::data_type::DataType;
    /// # use db::value::Value;
    /// let mut col = Column::new("age".into(), DataType::Int);
    /// col.push(Value::Int(30)).unwrap();
    /// col.push(Value::Null).unwrap();
    ///
    /// assert_eq!(col.len(), 2);
    /// assert!(col.get(1).unwrap().is_null());
    /// ```
    pub fn push(&mut self, value: Value) -> Result<(), String> {
        if value.is_null() {
            self.null_bitmap.push(true);
            // Add default value to keep alignment between the data vector and the bitmap
            match &mut self.data {
                ColumnData::Int(v) => v.push(0),
                ColumnData::Float(v) => v.push(0.0),
                ColumnData::Text(v) => v.push(String::new().into()),
                ColumnData::Bool(v) => v.push(false),
            }

            return Ok(());
        }

        if value.data_type() != Some(self.data_type) {
            return Err(format!(
                "Value {value:?} has type {:?} while column data type is {:?}",
                value.data_type(),
                self.data_type
            ));
        }

        self.null_bitmap.push(false);

        match (&mut self.data, value) {
            (ColumnData::Int(col), Value::Int(v)) => col.push(v),
            (ColumnData::Float(col), Value::Float(v)) => col.push(v),
            (ColumnData::Text(col), Value::Text(v)) => col.push(v),
            (ColumnData::Bool(col), Value::Bool(v)) => col.push(v),

            _ => {
                return Err("Internal error: type mismatch".into());
            }
        }

        Ok(())
    }

    /// Returns the number of rows currently stored in the column.
    pub fn len(&self) -> usize {
        self.null_bitmap.len()
    }

    /// Returns true if there is no row in the column, else false.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retrieves the value at the specified row index.
    ///
    /// Returns `Some(Value)` if the index is valid, or `None` if it is out of bounds.
    /// If the `null_bitmap` indicates a null at the index, `Some(Value::Null)` is returned.
    pub fn get(&self, row_idx: usize) -> Option<Value> {
        if row_idx >= self.len() {
            return None;
        }
        if self.null_bitmap[row_idx] {
            return Some(Value::Null);
        }
        match &self.data {
            ColumnData::Int(col) => Some(Value::Int(col[row_idx])),
            ColumnData::Text(col) => Some(Value::Text(col[row_idx].clone())),
            ColumnData::Float(col) => Some(Value::Float(col[row_idx])),
            ColumnData::Bool(col) => Some(Value::Bool(col[row_idx])),
        }
    }

    /// Remove the value at the specified row index.
    ///
    /// Returns `Ok(())` if the index is valid or an `Err(String)` if it is out of bounds
    pub fn remove(&mut self, row_idx: usize) -> Result<(), String> {
        // TODO Use a deletion vector later
        if self.len() <= row_idx {
            return Err("The row index is too high".into());
        }
        match &mut self.data {
            ColumnData::Int(col) => {
                col.remove(row_idx);
            }
            ColumnData::Text(col) => {
                col.remove(row_idx);
            }
            ColumnData::Bool(col) => {
                col.remove(row_idx);
            }
            ColumnData::Float(col) => {
                col.remove(row_idx);
            }
        }
        self.null_bitmap.remove(row_idx);
        Ok(())
    }

    /// Replace a value in the column by a new value.
    ///
    /// # Errors
    /// Returns an error if the row_idx is too high or if the value's type does not match the
    /// column's data type.
    ///
    /// # Behavior
    /// - If the new value is `Null`, the previous value is not changed but only the null_bitmap to
    ///   be faster.
    pub fn set(&mut self, row_idx: usize, value: &Value) -> Result<(), String> {
        if self.len() <= row_idx {
            return Err("The row index is too high".into());
        }

        // NULL handling
        if value.is_null() {
            self.null_bitmap.set(row_idx, true);
            // no need to change the value as null_bitmap is the first value checked at get.
            return Ok(());
        }

        if value.data_type() != Some(self.data_type) {
            return Err(format!(
                "Value {value:?} has type {:?} while column data type is {:?}",
                value.data_type(),
                self.data_type
            ));
        }

        self.null_bitmap.set(row_idx, false);
        match (&mut self.data, value) {
            (ColumnData::Int(col), Value::Int(v)) => col[row_idx] = *v,
            (ColumnData::Float(col), Value::Float(v)) => col[row_idx] = *v,
            (ColumnData::Text(col), Value::Text(v)) => col[row_idx] = Arc::clone(v),
            (ColumnData::Bool(col), Value::Bool(v)) => {
                col.replace(row_idx, *v);
            }
            _ => {
                return Err("Internal error: type mismatch".into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::DataType;
    use crate::value::Value;

    // ─────────────────────────────────────────────────────────────
    // Test 1 : Creation
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_column_new() {
        let col = Column::new("age".into(), DataType::Int);

        // Nom correct
        assert_eq!(col.name, "age");

        // Type correct
        assert_eq!(col.data_type, DataType::Int);

        // Colonne vide
        assert_eq!(col.len(), 0);

        // Bitmap NULL vide
        assert_eq!(col.null_bitmap.len(), 0);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 2 : Basic Push & Get
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_push_and_get() {
        let mut col = Column::new("test".into(), DataType::Int);

        col.push(Value::Int(42)).unwrap();

        assert_eq!(col.len(), 1);
        assert_eq!(col.get(0), Some(Value::Int(42)));
        assert!(!col.null_bitmap[0]);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 3 : NULL
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_null_handling() {
        let mut col = Column::new("nullable".into(), DataType::Int);

        col.push(Value::Int(10)).unwrap();
        col.push(Value::Null).unwrap();
        col.push(Value::Int(20)).unwrap();

        // Longueur correcte
        assert_eq!(col.len(), 3);

        // Valeurs
        assert_eq!(col.get(0), Some(Value::Int(10)));
        assert_eq!(col.get(1), Some(Value::Null));
        assert_eq!(col.get(2), Some(Value::Int(20)));

        // Bitmap NULL
        assert!(!col.null_bitmap[0]);
        assert!(col.null_bitmap[1]);
        assert!(!col.null_bitmap[2]);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 4 : Type mismatch
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_type_mismatch() {
        let mut col = Column::new("int_col".into(), DataType::Int);

        let result = col.push(Value::Text("hello".into()));

        assert!(result.is_err());
        assert_eq!(col.len(), 0); // aucune insertion
    }

    // ─────────────────────────────────────────────────────────────
    // Test 5 : Out of bounds
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_out_of_bounds() {
        let col = Column::new("test".into(), DataType::Int);

        // Colonne vide → None
        assert_eq!(col.get(0), None);
        assert_eq!(col.get(100), None);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 6: Large Column
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_large_column() {
        let mut col = Column::new("big".into(), DataType::Int);

        for i in 0..10_000 {
            col.push(Value::Int(i)).unwrap();
        }

        assert_eq!(col.len(), 10_000);
        assert_eq!(col.get(5_000), Some(Value::Int(5_000)));
        assert_eq!(col.get(9_999), Some(Value::Int(9_999)));
    }

    // ─────────────────────────────────────────────────────────────
    // Test 7 : column full of Null
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_all_nulls() {
        let mut col = Column::new("nulls".into(), DataType::Int);

        for _ in 0..100 {
            col.push(Value::Null).unwrap();
        }

        assert_eq!(col.len(), 100);

        for i in 0..100 {
            assert!(col.get(i).unwrap().is_null());
            assert!(col.null_bitmap[i]);
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Test 8 : remove a value
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_delete() {
        let mut col = Column::new("test".into(), DataType::Int);

        col.push(Value::Int(42)).unwrap();
        col.push(Value::Int(59)).unwrap();
        col.push(Value::Null).unwrap();

        assert_eq!(col.len(), 3);

        col.remove(1).unwrap();
        assert_eq!(col.len(), 2);
        assert_eq!(col.get(0), Some(Value::Int(42)));
        assert!(!col.null_bitmap[0]);
        assert_eq!(col.get(1), Some(Value::Null));
        assert!(col.null_bitmap[1]);
    }

    // ─────────────────────────────────────────────────────────────
    // Test 9 : change a value
    // ─────────────────────────────────────────────────────────────
    #[test]
    fn test_column_set() {
        let mut col = Column::new("age".into(), DataType::Int);
        col.push(Value::Int(30)).unwrap();
        col.push(Value::Null).unwrap();

        col.set(0, &Value::Int(31)).unwrap();
        assert_eq!(col.get(0), Some(Value::Int(31)));

        col.set(1, &Value::Int(25)).unwrap();
        assert_eq!(col.get(1), Some(Value::Int(25)));
        assert!(!col.null_bitmap[1]);

        col.set(0, &Value::Null).unwrap();
        assert_eq!(col.get(0), Some(Value::Null));
        assert!(col.null_bitmap[0]);

        assert!(col.set(0, &Value::Text("hello".into())).is_err());

        assert!(col.set(10, &Value::Int(42)).is_err());
    }
}
