use std::sync::Arc;

use crate::tokenizer::Token;
use crate::{ColumnDef, DataType, Value, ast::*};

/// A recursive descent parser that transforms a sequence of [Token]s
/// into an Abstract Syntax Tree (AST) represented by a [Statement].
pub struct Parser {
    /// The stream of tokens produced by the Tokenizer.
    tokens: Vec<Token>,
    /// The current index in the token stream.
    position: usize,
}

impl Parser {
    /// Creates a new parser from a list of tokens.
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    /// Parses the tokens into a single [Statement].
    ///
    /// This is the main entry point of the parser. It identifies the statement type
    /// (CREATE, INSERT, or SELECT) and ensures the entire input is consumed.
    ///
    /// # Errors
    /// Returns an error string if the syntax is invalid or if trailing tokens
    /// remain after a valid statement.
    pub fn parse(&mut self) -> Result<Statement, String> {
        let statement = match self.current_token() {
            Token::Create => self.parse_create_table(),
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            _ => Err(format!("Unexpected token: {:?}", self.current_token())),
        }?;

        // Semicolons are optional at the end of a statement
        if matches!(self.current_token(), Token::Semicolon) {
            self.advance();
        }

        // Check we are at the end of the statement to avoid ignored trailing syntax
        if !self.is_at_end() {
            return Err(format!(
                "Unexpected token after statement: {:?}",
                self.current_token()
            ));
        }

        Ok(statement)
    }

    // --- Navigation Helpers ---

    /// Returns a reference to the token at the current position.
    fn current_token(&self) -> &Token {
        &self.tokens[self.position]
    }

    /// Advances the cursor to the next token.
    fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }

    /// Checks if the cursor has reached the end of the token stream.
    fn is_at_end(&self) -> bool {
        matches!(self.current_token(), Token::Eof)
    }

    /// Validates that the current token matches the expected one and advances.
    /// Returns an error if the token doesn't match.
    fn consume(&mut self, expected: Token) -> Result<(), String> {
        if *self.current_token() == expected {
            self.advance();
            Ok(())
        } else {
            Err(format!(
                "Expected {:?}, found {:?}",
                expected,
                self.current_token()
            ))
        }
    }

    /// Specifically consumes an [Token::Ident] and returns its inner string.
    fn consume_ident(&mut self) -> Result<String, String> {
        match self.current_token() {
            Token::Ident(string) => {
                let string = string.clone();
                self.advance();
                Ok(string)
            }
            _ => Err(format!(
                "Expected identifier, found {:?}",
                self.current_token()
            )),
        }
    }

    /// Consumes a literal token (Number, String, Bool) and converts it to a [Value].
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
            _ => Err(format!("Expected value, found {:?}", self.current_token())),
        }
    }

    /// Maps a keyword token (like INT, TEXT) to a logical [DataType].
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
                "Current token {:?} is not a supported data type",
                self.current_token()
            )),
        }
    }

    // --- Production Rules ---

    /// Parses a column definition in a `CREATE TABLE` statement (e.g., `id INT`).
    fn parse_column_def(&mut self) -> Result<ColumnDef, String> {
        let name = self.consume_ident()?;
        let data_type = self.consume_data_type()?;
        Ok(ColumnDef { name, data_type })
    }

    /// Parses a full `CREATE TABLE` statement.
    fn parse_create_table(&mut self) -> Result<Statement, String> {
        self.consume(Token::Create)?;
        self.consume(Token::Table)?;
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

    /// Parses an `INSERT INTO` statement, handling optional column lists.
    fn parse_insert(&mut self) -> Result<Statement, String> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;
        let name = self.consume_ident()?;

        // Columns are optional: INSERT INTO table (col1, col2) ... OR INSERT INTO table ...
        let columns = if matches!(self.current_token(), Token::LeftParen) {
            self.advance();
            let mut cols = vec![];
            loop {
                cols.push(self.consume_ident()?);
                match self.current_token() {
                    Token::Comma => self.advance(),
                    Token::RightParen => {
                        self.advance();
                        break;
                    }
                    _ => return Err("Expected ',' or ')'".into()),
                }
            }
            Some(cols)
        } else {
            None
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
                _ => return Err("Expected ',' or ')'".into()),
            };
        }

        Ok(Statement::InsertInto(InsertInto {
            table: name,
            columns,
            values,
        }))
    }

    /// Parses the column selection part of a `SELECT` statement (e.g., `*` or `col1, col2`).
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

    /// Parses a `SELECT` statement.
    fn parse_select(&mut self) -> Result<Statement, String> {
        self.consume(Token::Select)?;
        let columns = self.parse_columns()?;
        self.consume(Token::From)?;
        let table = self.consume_ident()?;
        let where_clause: Option<Expr> = {
            if *self.current_token() == Token::Where {
                self.advance();
                Some(self.parse_expression()?)
            } else {
                None
            }
        };
        let limit = {
            if *self.current_token() == Token::Limit {
                self.advance();
                match self.current_token() {
                    Token::Number(n) => {
                        let limit = *n as usize; // i64 → usize
                        self.advance();
                        Some(limit)
                    }
                    _ => return Err("LIMIT requires a number".into()),
                }
            } else {
                None
            }
        };

        let order_by = {
            if *self.current_token() == Token::Order {
                self.advance();
                self.consume(Token::By)?;
                Some(self.parse_order_by()?)
            } else {
                None
            }
        };

        Ok(Statement::Select(Select {
            columns,
            table,
            where_clause,
            limit,
            order_by,
        }))
    }

    /// Parses a WHERE clause expression.
    ///
    /// Entry point for expression parsing. Delegates to `parse_or_expr()`
    /// to handle operator precedence correctly.
    fn parse_expression(&mut self) -> Result<Expr, String> {
        self.parse_or_expr()
    }

    /// Parses OR expressions with left-associativity.
    ///
    /// Handles chains of OR operations, building a left-associative tree.
    /// OR has the lowest precedence among boolean operators.
    ///
    /// # Examples
    /// - `age > 18 OR name = 'Alice'`
    /// - `a = 1 OR b = 2 OR c = 3` → `((a=1) OR (b=2)) OR (c=3)`
    fn parse_or_expr(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_and_expr()?;
        while matches!(self.current_token(), Token::Or) {
            self.advance();
            let right_expr = self.parse_and_expr()?;
            expr = Expr::Or {
                left: Box::new(expr),
                right: Box::new(right_expr),
            };
        }
        Ok(expr)
    }

    /// Parses AND expressions with left-associativity.
    ///
    /// Handles chains of AND operations, building a left-associative tree.
    /// AND has higher precedence than OR but lower than comparisons.
    ///
    /// # Examples
    /// - `age > 18 AND active = TRUE`
    /// - `a = 1 AND b = 2 AND c = 3` → `((a=1) AND (b=2)) AND (c=3)`
    fn parse_and_expr(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_op_expr()?;
        while matches!(self.current_token(), Token::And) {
            self.advance();
            let right_expr = self.parse_op_expr()?;
            expr = Expr::And {
                left: Box::new(expr),
                right: Box::new(right_expr),
            };
        }
        Ok(expr)
    }

    /// Parses a comparison expression (the atomic unit of WHERE clauses).
    ///
    /// Expects the pattern: `column OPERATOR value`
    ///
    /// # Supported Operators
    /// - `>` (Greater Than)
    /// - `<` (Less Than)
    /// - `=` (Equal)
    ///
    /// # Examples
    /// - `age > 18`
    /// - `name = 'Alice'`
    /// - `active = TRUE`
    fn parse_op_expr(&mut self) -> Result<Expr, String> {
        let column = self.consume_ident()?;
        let op = match self.current_token() {
            Token::Lower => ComparisonOp::Lt,
            Token::Greater => ComparisonOp::Gt,
            Token::Equal => ComparisonOp::Eq,
            _ => {
                return Err(format!(
                    "Token {:?} is not a valid operator",
                    self.current_token()
                ));
            }
        };
        self.advance();
        let value = self.consume_value()?;
        Ok(Expr::Comparison { column, op, value })
    }

    /// Parses an `ORDER BY` clause.
    ///
    /// # Default Behavior
    /// If no direction (`ASC` or `DESC`) is specified, defaults to `ASC`
    /// per SQL standard.
    ///
    /// # Returns
    /// A vector of [OrderByClause] in the order they should be applied.
    /// The first clause has priority in sorting.
    fn parse_order_by(&mut self) -> Result<Vec<OrderByClause>, String> {
        let mut clauses = vec![];

        loop {
            let column = self.consume_ident()?;

            let direction = match self.current_token() {
                Token::Asc => {
                    self.advance();
                    SortDirection::Asc
                }
                Token::Desc => {
                    self.advance();
                    SortDirection::Desc
                }
                _ => SortDirection::Asc, // ASC by default
            };

            clauses.push(OrderByClause { column, direction });

            if matches!(self.current_token(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(clauses)
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

    #[test]
    fn test_parse_select_with_where() {
        let sql = "SELECT * FROM users WHERE age > 18";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select(sel) => {
                assert_eq!(sel.table, "users");
                assert!(sel.where_clause.is_some());

                if let Some(Expr::Comparison { column, op, value }) = sel.where_clause {
                    assert_eq!(column, "age");
                    assert_eq!(op, ComparisonOp::Gt);
                    assert_eq!(value, Value::Int(18));
                } else {
                    panic!("Expected Comparison");
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_with_and() {
        let sql = "SELECT name FROM users WHERE age > 18 AND active = TRUE";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select(sel) => {
                assert!(matches!(sel.where_clause, Some(Expr::And { .. })));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_single_order_by() {
        let sql = "SELECT name FROM users ORDER BY age ASC";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select(sel) => {
                let order_by = sel.order_by.unwrap();
                assert_eq!(order_by.len(), 1);
                assert_eq!(order_by[0].column, "age");
                assert_eq!(order_by[0].direction, SortDirection::Asc);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_parse_select_multiple_order_by() {
        let sql = "SELECT name FROM users ORDER BY age ASC, name DESC";
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize().unwrap();

        let mut parser = Parser::new(tokens);
        let statement = parser.parse().unwrap();

        match statement {
            Statement::Select(sel) => {
                let order_by = sel.order_by.unwrap();
                assert_eq!(order_by.len(), 2);

                assert_eq!(order_by[0].column, "age");
                assert_eq!(order_by[0].direction, SortDirection::Asc);

                assert_eq!(order_by[1].column, "name");
                assert_eq!(order_by[1].direction, SortDirection::Desc);
            }
            _ => panic!("Expected Select"),
        }
    }
}
