#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // keywords
    Create,
    Table,
    Primary,
    Key,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    Delete,

    // identifiers + literals
    Ident(String),
    Int(i64),
    Float(f64),
    String(String),

    // symbols
    LParen,
    RParen,
    Comma,
    Semicolon,
    Star,
    Eq,

    EOF,
}

pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(c) if c.is_whitespace()) {
            self.advance();
        }
    }

    fn read_ident(&mut self) -> String {
        let start = self.pos;

        while matches!(self.peek(), Some(c) if c.is_alphanumeric() || c == '_') {
            self.advance();
        }

        self.input[start..self.pos].iter().collect()
    }

    fn keyword_or_ident(ident: String) -> Token {
        match ident.to_uppercase().as_str() {
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "PRIMARY" => Token::Primary,
            "KEY" => Token::Key,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "DELETE" => Token::Delete,
            _ => Token::Ident(ident),
        }
    }

    fn read_number(&mut self) -> Token {
        let start = self.pos;
        let mut has_dot = false;

        while let Some(c) = self.peek() {
            if c == '.' {
                has_dot = true;
            } else if !c.is_ascii_digit() {
                break;
            }
            self.advance();
        }

        let s: String = self.input[start..self.pos].iter().collect();

        if has_dot {
            Token::Float(s.parse().unwrap())
        } else {
            Token::Int(s.parse().unwrap())
        }
    }

    fn read_string(&mut self) -> Result<Token, String> {
        // skip opening "
        self.advance();
        let start = self.pos;

        while let Some(c) = self.peek() {
            if c == '"' {
                let s: String = self.input[start..self.pos].iter().collect();
                self.advance();  // closing "
                return Ok(Token::String(s));
            }
            self.advance();
        }

        Err("Undetermined string literal".into())
    }

    pub fn next_token(&mut self) -> Result<Token, String> {
        self.skip_whitespace();

        let ch = match self.peek() {
            Some(c) => c,
            None => return Ok(Token::EOF),
        };

        let token = match ch {
            '(' => { self.advance(); Token::LParen }
            ')' => { self.advance(); Token::RParen }
            ',' => { self.advance(); Token::Comma }
            ';' => { self.advance(); Token::Semicolon }
            '*' => { self.advance(); Token::Star }
            '=' => { self.advance(); Token::Eq }

            '"' => return self.read_string(),

            c if c.is_ascii_digit() => self.read_number(),

            c if c.is_alphabetic() || c == '_' => {
                let ident = self.read_ident();
                Self::keyword_or_ident(ident)
            }

            _ => return Err(format!("Unexpected character: {}", ch)),
        };

        Ok(token)
    }
}
