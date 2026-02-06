/// Represents the smallest meaningful units (atoms) of the SQL language.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // --- SQL Keywords ---
    Create,
    Table,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    And,
    Or,
    Limit,
    Order,
    By,
    Asc,
    Desc,

    // --- Data Types ---
    Int,
    Text,
    Float,
    Bool,

    // --- Identifiers & Literals ---
    /// A name representing a table or a column (e.g., `users`, `id`).
    Ident(String),
    /// A 64-bit integer literal (e.g., `42`).
    Number(i64),
    /// A string literal, defined between single quotes (e.g., `'Alice'`).
    String(String),
    /// A 64-bit floating-point literal (e.g., `3.14`).
    FloatNumber(f64),
    /// The boolean literal `TRUE`.
    True,
    /// The boolean literal `FALSE`.
    False,

    // --- Symbols ---
    /// Left parenthesis `(`
    LeftParen,
    /// Right parenthesis `)`
    RightParen,
    /// Comma `,`
    Comma,
    /// Semicolon `;`
    Semicolon,
    /// Multiplication or wildcard symbol `*`
    Star,
    /// Greater than
    Greater,
    /// Lower than
    Lower,
    /// Equal to
    Equal,

    // --- Special ---
    /// Represents the End Of File/Input.
    Eof,
}

/// A lexical scanner (lexer) that converts a raw SQL string into a sequence of [Token]s.
pub struct Tokenizer {
    /// The input string stored as a vector of characters for easy iteration.
    input: Vec<char>,
    /// The current position in the character vector.
    position: usize,
}

impl Tokenizer {
    /// Creates a new Tokenizer for the given input string.
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    /// Processes the entire input and returns a vector of tokens.
    ///
    /// # Errors
    /// Returns an error if an invalid character is encountered or if a literal
    /// (like a string) is malformed.
    ///
    /// # Example
    /// ```
    /// # use db::tokenizer::{Tokenizer, Token};
    /// let mut t = Tokenizer::new("SELECT *");
    /// let tokens = t.tokenize().unwrap();
    /// assert_eq!(tokens[0], Token::Select);
    /// ```
    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();

        while !self.is_at_end() {
            self.skip_whitespace();

            if self.is_at_end() {
                break;
            }

            let token = self.next_token()?;
            tokens.push(token);
        }

        tokens.push(Token::Eof);
        Ok(tokens)
    }

    /// Identifies the next token based on the character at the current position.
    fn next_token(&mut self) -> Result<Token, String> {
        let ch = self.current_char();

        match ch {
            '(' => {
                self.advance();
                Ok(Token::LeftParen)
            }
            ')' => {
                self.advance();
                Ok(Token::RightParen)
            }
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }
            '*' => {
                self.advance();
                Ok(Token::Star)
            }
            '>' => {
                self.advance();
                Ok(Token::Greater)
            }
            '<' => {
                self.advance();
                Ok(Token::Lower)
            }
            '=' => {
                self.advance();
                Ok(Token::Equal)
            }
            c if c.is_alphabetic() => self.read_identifier(),
            c if c.is_numeric() => self.read_number(),
            '\'' => self.read_string(),
            _ => Err(format!("character: {:?} is not supported", ch)),
        }
    }

    // --- Navigation Helpers ---

    /// Returns the character at the current position.
    fn current_char(&self) -> char {
        self.input[self.position]
    }

    /// Moves the cursor forward by one character.
    fn advance(&mut self) {
        self.position += 1;
    }

    /// Checks if the cursor has reached the end of the input.
    fn is_at_end(&self) -> bool {
        self.position >= self.input.len()
    }

    /// Consumes any whitespace characters (spaces, tabs, newlines).
    fn skip_whitespace(&mut self) {
        while !self.is_at_end() && self.current_char().is_whitespace() {
            self.advance();
        }
    }

    // --- Extraction Logic ---

    /// Reads a sequence of alphanumeric characters and determines if it's
    /// a reserved SQL keyword or a user-defined identifier.
    ///
    /// Keywords are matched case-insensitively.
    fn read_identifier(&mut self) -> Result<Token, String> {
        let mut ident = String::new();

        while !self.is_at_end()
            && (self.current_char().is_alphanumeric() || self.current_char() == '_')
        {
            ident.push(self.current_char());
            self.advance();
        }

        match ident.to_uppercase().as_str() {
            "CREATE" => Ok(Token::Create),
            "TABLE" => Ok(Token::Table),
            "INSERT" => Ok(Token::Insert),
            "INTO" => Ok(Token::Into),
            "VALUES" => Ok(Token::Values),
            "SELECT" => Ok(Token::Select),
            "FROM" => Ok(Token::From),
            "WHERE" => Ok(Token::Where),
            "INT" => Ok(Token::Int),
            "TEXT" => Ok(Token::Text),
            "FLOAT" => Ok(Token::Float),
            "BOOL" => Ok(Token::Bool),
            "TRUE" => Ok(Token::True),
            "FALSE" => Ok(Token::False),
            "AND" => Ok(Token::And),
            "OR" => Ok(Token::Or),
            "LIMIT" => Ok(Token::Limit),
            "ORDER" => Ok(Token::Order),
            "BY" => Ok(Token::By),
            "ASC" => Ok(Token::Asc),
            "DESC" => Ok(Token::Desc),
            _ => Ok(Token::Ident(ident)),
        }
    }

    /// Reads a numeric literal. If a dot `.` is encountered, it returns a
    /// [Token::FloatNumber], otherwise a [Token::Number].
    fn read_number(&mut self) -> Result<Token, String> {
        let mut number = String::new();
        let mut has_dot = false;

        while !self.is_at_end()
            && (self.current_char().is_numeric() || (self.current_char() == '.' && !has_dot))
        {
            if self.current_char() == '.' {
                has_dot = true;
            }
            number.push(self.current_char());
            self.advance();
        }

        if !self.is_at_end() && self.current_char() == '.' {
            return Err("multiple dots are not allowed for a float".into());
        }

        if has_dot {
            return number
                .parse::<f64>()
                .map(Token::FloatNumber)
                .map_err(|e| e.to_string());
        }

        number
            .parse::<i64>()
            .map(Token::Number)
            .map_err(|e| e.to_string())
    }

    /// Reads a string literal enclosed in single quotes.
    fn read_string(&mut self) -> Result<Token, String> {
        self.advance(); // Skip the opening quote

        if !self.is_at_end() && self.current_char() == '\'' {
            self.advance(); // Consume empty string closing quote
            return Ok(Token::String(String::new()));
        }

        let mut string = String::new();
        while !self.is_at_end() && self.current_char() != '\'' {
            string.push(self.current_char());
            self.advance();
        }

        if self.is_at_end() {
            return Err("Unterminated string".into());
        }

        // Skip the closing quote
        self.advance();

        Ok(Token::String(string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_simple() {
        let mut tokenizer = Tokenizer::new("CREATE TABLE users");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Create,
                Token::Table,
                Token::Ident("users".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_tokenize_with_parens() {
        let mut tokenizer = Tokenizer::new("(id, name)");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::LeftParen,
                Token::Ident("id".into()),
                Token::Comma,
                Token::Ident("name".into()),
                Token::RightParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_tokenize_create_table() {
        let mut tokenizer = Tokenizer::new("CREATE TABLE users (id INT, name TEXT)");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Create,
                Token::Table,
                Token::Ident("users".into()),
                Token::LeftParen,
                Token::Ident("id".into()),
                Token::Int,
                Token::Comma,
                Token::Ident("name".into()),
                Token::Text,
                Token::RightParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_tokenize_numbers() {
        let mut tokenizer = Tokenizer::new("42, 123, 0");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::Number(42),
                Token::Comma,
                Token::Number(123),
                Token::Comma,
                Token::Number(0),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_tokenize_floats() {
        let mut tokenizer = Tokenizer::new("1.14, 45.0");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::FloatNumber(1.14),
                Token::Comma,
                Token::FloatNumber(45.0),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_tokenize_strings() {
        let mut tokenizer = Tokenizer::new("'Alice', 'Bob Dylan'");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(
            tokens,
            vec![
                Token::String("Alice".into()),
                Token::Comma,
                Token::String("Bob Dylan".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_unterminated_string() {
        let mut tokenizer = Tokenizer::new("'hello");
        let result = tokenizer.tokenize();

        assert!(result.is_err());
    }
}
