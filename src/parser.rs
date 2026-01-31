use std::sync::Arc;

use crate::tokenizer::Token;
use crate::{ColumnDef, DataType, Value, ast::*};

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    pub fn parse(&mut self) -> Result<Statement, String> {
        let statement = match self.current_token() {
            Token::Create => self.parse_create_table(),
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            _ => Err(format!("Unexpected token: {:?}", self.current_token())),
        }?;

        // semicolon is optionnal in SQL so skip it
        if matches!(self.current_token(), Token::Semicolon) {
            self.advance();
        }

        // Check we are at the end of the statement
        if !self.is_at_end() {
            return Err(format!(
                "Unexpected token after statement: {:?}",
                self.current_token()
            ));
        }

        Ok(statement)
    }

    //helpers
    fn current_token(&self) -> &Token {
        &self.tokens[self.position]
    }

    fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }

    fn is_at_end(&self) -> bool {
        matches!(self.current_token(), Token::Eof)
    }

    fn consume(&mut self, expected: Token) -> Result<(), String> {
        if *self.current_token() == expected {
            self.advance();
            Ok(())
        } else {
            // Une erreur précise aide énormément au debug
            Err(format!(
                "Expected {:?}, found {:?}",
                expected,
                self.current_token()
            ))
        }
    }

    fn consume_ident(&mut self) -> Result<String, String> {
        match self.current_token() {
            Token::Ident(string) => {
                // TODO: Should be zero-copy with references in token
                let string = string.clone(); // Get the name
                self.advance();
                Ok(string)
            }
            _ => Err(format!(
                "Expected identifier, found {:?}",
                self.current_token()
            )),
        }
    }

    fn consume_value(&mut self) -> Result<Value, String> {
        match self.current_token() {
            Token::Number(nb) => {
                let nb_copy = *nb;
                self.advance();
                Ok(Value::Int(nb_copy))
            }
            Token::FloatNumber(nb) => {
                let nb_copy = *nb;
                self.advance();
                Ok(Value::Float(nb_copy))
            }
            Token::True => {
                self.advance();
                Ok(Value::Bool(true))
            }
            Token::False => {
                self.advance();
                Ok(Value::Bool(false))
            }
            Token::String(string) => {
                let text = Arc::from(string.as_str());
                self.advance();
                Ok(Value::Text(text))
            }
            _ => Err(format!("Expected number, found {:?}", self.current_token())),
        }
    }

    fn consume_data_type(&mut self) -> Result<DataType, String> {
        match self.current_token() {
            Token::Int => {
                self.advance();
                Ok(DataType::Int)
            }
            Token::Text => {
                self.advance();
                Ok(DataType::Text)
            }
            Token::Bool => {
                self.advance();
                Ok(DataType::Bool)
            }
            Token::Float => {
                self.advance();
                Ok(DataType::Float)
            }
            _ => Err(format!(
                "current token {:?} is not a column type",
                self.current_token()
            )),
        }
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, String> {
        let name = self.consume_ident()?;

        let data_type = self.consume_data_type()?;

        Ok(ColumnDef { name, data_type })
    }

    fn parse_create_table(&mut self) -> Result<Statement, String> {
        self.consume(Token::Create)?; // advance if CREATE
        self.consume(Token::Table)?; // advance if TABLE
        let name = self.consume_ident()?;
        self.consume(Token::LeftParen)?;
        let mut columns = vec![];
        loop {
            columns.push(self.parse_column_def()?);
            match self.current_token() {
                Token::RightParen => {
                    self.advance();
                    break;
                }
                Token::Comma => {
                    self.advance();
                    continue;
                }
                _ => return Err("Expected ',' or ')'".into()),
            }
        }
        Ok(Statement::CreateTable(CreateTable { name, columns }))
    }

    fn parse_insert(&mut self) -> Result<Statement, String> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;
        let name = self.consume_ident()?;

        // Columns are optionnal in SQL
        let columns = if matches!(self.current_token(), Token::LeftParen) {
            self.advance();
            let mut cols = vec![];

            loop {
                cols.push(self.consume_ident()?);

                match self.current_token() {
                    Token::Comma => {
                        self.advance();
                    }
                    Token::RightParen => {
                        self.advance();
                        break;
                    }
                    _ => return Err("Expected ',' or ')'".into()),
                }
            }

            Some(cols)
        } else {
            None // no specified columns, use schema order
        };

        self.consume(Token::Values)?;
        self.consume(Token::LeftParen)?;
        let mut values = vec![];
        loop {
            values.push(self.consume_value()?);
            match self.current_token() {
                Token::Comma => self.advance(),
                Token::RightParen => {
                    self.advance();
                    break;
                }
                _ => {
                    return Err("Expected ',' or ')'".into());
                }
            };
        }
        Ok(Statement::InsertInto(InsertInto {
            table: name,
            columns,
            values,
        }))
    }

    fn parse_columns(&mut self) -> Result<ColumnsSelect, String> {
        match self.current_token() {
            Token::Star => {
                self.advance();
                Ok(ColumnsSelect::Star)
            }

            Token::Ident(_) => {
                let mut cols = Vec::new();

                loop {
                    match self.current_token() {
                        Token::Ident(name) => {
                            cols.push(name.clone());
                            self.advance();
                        }
                        _ => return Err("Expected column name".into()),
                    }

                    if *self.current_token() == Token::Comma {
                        self.advance();
                        continue;
                    }

                    break;
                }

                Ok(ColumnsSelect::ColumnsNames(cols))
            }

            _ => Err("Expected '*' or column name".into()),
        }
    }

    fn parse_select(&mut self) -> Result<Statement, String> {
        self.consume(Token::Select)?;
        let columns = self.parse_columns()?;
        self.consume(Token::From)?;
        let table = self.consume_ident()?;
        Ok(Statement::Select(Select { columns, table }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::Tokenizer;

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE users (id INT, name TEXT)";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.name, "users");
                assert_eq!(ct.columns.len(), 2);
                assert_eq!(ct.columns[0].name, "id");
                assert_eq!(ct.columns[0].data_type, DataType::Int);
                assert_eq!(ct.columns[1].name, "name");
                assert_eq!(ct.columns[1].data_type, DataType::Text);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::InsertInto(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(ins.columns, Some(vec!["id".into(), "name".into()]));
                assert_eq!(ins.values.len(), 2);
                assert_eq!(ins.values[0], Value::Int(1));
                assert_eq!(ins.values[1], Value::Text(Arc::from("Alice")));
            }
            _ => panic!("Expected InsertInto"),
        }
    }

    #[test]
    fn test_parse_insert_no_columns() {
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::InsertInto(ins) => {
                assert_eq!(ins.table, "users");
                assert_eq!(ins.columns, None);
                assert_eq!(ins.values.len(), 2);
                assert_eq!(ins.values[0], Value::Int(1));
                assert_eq!(ins.values[1], Value::Text(Arc::from("Alice")));
            }
            _ => panic!("Expected InsertInto"),
        }
    }

    #[test]
    fn test_select_star() {
        let sql = "SELECT * FROM users";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select(sel) => {
                assert_eq!(sel.columns, ColumnsSelect::Star);
                assert_eq!(sel.table, "users".to_string());
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_select_columns() {
        let sql = "SELECT name, age FROM users";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select(sel) => {
                let columns = vec!["name".into(), "age".into()];
                assert_eq!(sel.columns, ColumnsSelect::ColumnsNames(columns));
                assert_eq!(sel.table, "users".to_string());
            }
            _ => panic!("Expected Select"),
        }
    }
}
