use crate::data_type::DataType;
use crate::value::Value;
use bitvec::prelude::*;

#[derive(Debug, Clone)]
pub enum ColumnData {
    Int(Vec<i64>),
    Float(Vec<f64>),
    Text(Vec<String>),
    Bool(BitVec), // ← Déjà compact !
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub data: ColumnData,
    pub null_bitmap: BitVec,
}

impl Column {
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

    pub fn push(&mut self, value: Value) -> Result<(), String> {
        if value.is_null() {
            self.null_bitmap.push(true);
            // add default value to keep alignment between vec and bitvec
            match &mut self.data {
                ColumnData::Int(v) => v.push(0),
                ColumnData::Float(v) => v.push(0.0),
                ColumnData::Text(v) => v.push(String::new()),
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

    pub fn len(&self) -> usize {
        self.null_bitmap.len()
    }

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
    // Test 7 : colonn full of Null
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
}
