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
