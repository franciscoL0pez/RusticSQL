use crate::errors::{self, SqlError};

///Enum para representar los distintos tipos de operadores logicos
#[derive(PartialEq,Debug)]
pub enum OpLogico {
    And,
    Or,
    Not,
}

/// Returns true if the token is equal to "AND".
pub fn is_and(token: &str) -> bool {
    token == "AND"
}

/// Returns true if the token is equal to "OR".
pub fn is_or(token: &str) -> bool {
    token == "OR"
}

/// Returns true if the token is equal to "NOT".
pub fn is_not(token: &str) -> bool {
    token == "NOT"
}

//Funcion para obtener el tipo de operador logico
pub fn obtener_op_logico(op: &str) -> Result<OpLogico, SqlError> {
    match op {
        "AND" => Ok(OpLogico::And),
        "OR" => Ok(OpLogico::Or),
        "NOT" => Ok(OpLogico::Not),
        
        _ => Err(errors::SqlError::InvalidSyntax),
    }
}