//! # In-Memory Database Engine
//!
//! A lightweight, columnar-based SQL database engine that operates entirely in RAM.
//! This crate provides tools for tokenizing, parsing, and executing SQL-like statements
//! against an efficient in-memory storage system.

/// Definitions for the Abstract Syntax Tree (AST) nodes.
pub mod ast;
/// Core logic for columnar data storage and bitmapped null handling.
pub mod column;
/// Definitions of the supported logical data types.
pub mod data_type;
/// The main database engine and execution orchestrator.
pub mod database;
/// Logic for converting tokens into an executable AST.
pub mod parser;
/// Table and Schema definitions and management logic.
pub mod table;
/// Lexical scanner that converts raw SQL strings into tokens.
pub mod tokenizer;
/// Representation of individual data values and nulls.
pub mod value;

// Re-exporting primary types for a cleaner public API.
// This allows users to use `your_crate::Database` instead of `your_crate::database::Database`.

pub use column::Column;
pub use data_type::DataType;
pub use database::Database;
pub use table::{ColumnDef, Schema, Table};
pub use value::Value;
