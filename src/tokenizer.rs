#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // SQL Keywords
    Create,
    Table,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,

    // Types
    Int,
    Text,
    Float,
    Bool,

    // Identifiers & Literals
    Ident(String),  // nom de table/colonne
    Number(i64),    // 42
    String(String), // 'Alice'
    FloatNumber(f64),
    True,
    False,

    // Symbols
    LeftParen,  // (
    RightParen, // )
    Comma,      // ,
    Semicolon,  // ;
    Star,       // *

    // EOF
    Eof,
}

pub struct Tokenizer {
    input: Vec<char>,
    position: usize,
}

impl Tokenizer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

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
            c if c.is_alphabetic() => self.read_identifier(),
            c if c.is_numeric() => self.read_number(),
            '\'' => self.read_string(),
            _ => Err(format!("character: {:?} is not supported", ch)),
        }
    }

    // Helpers
    fn current_char(&self) -> char {
        self.input[self.position]
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn is_at_end(&self) -> bool {
        self.position >= self.input.len()
    }

    fn skip_whitespace(&mut self) {
        while !self.is_at_end() && self.current_char().is_whitespace() {
            self.advance();
        }
    }

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
            _ => Ok(Token::Ident(ident)),
        }
    }

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
            return Err("mutliple . is not allowed for a float".into());
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

    fn read_string(&mut self) -> Result<Token, String> {
        self.advance();
        if self.current_char() == '\'' {
            return Err("The string passed in entry is null".to_string());
        }
        let mut string = String::new();
        while self.current_char() != '\'' && !self.is_at_end() {
            string.push(self.current_char());
            self.advance();
            if self.is_at_end() {
                return Err("Unterminated string".into());
            }
        }

        // Skip the last '
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
