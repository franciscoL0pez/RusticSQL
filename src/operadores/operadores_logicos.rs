use crate::errors::{self, SqlError};

///Enum para representar los distintos tipos de operadores logicos
#[derive(PartialEq, Debug)]
pub enum OpLogico {
    And,
    Or,
    Not,
}

/// Retorna true si el token "AND".
pub fn es_and(token: &str) -> bool {
    token == "AND"
}

/// Retorna true si el token es "OR".
pub fn es_or(token: &str) -> bool {
    token == "OR"
}

/// Retorna true si el token es "NOT".
pub fn es_not(token: &str) -> bool {
    token == "NOT"
}
