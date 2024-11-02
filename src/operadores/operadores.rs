use crate::errors::{self, SqlError};

///Funcion para obtener el tipo de operador
pub fn obtener_op(op: &str) -> Result<Operador, SqlError> {
    match op {
        "=" => Ok(Operador::Igual),
        "<" => Ok(Operador::Menor),
        ">" => Ok(Operador::Mayor),

        _ => Err(errors::SqlError::InvalidSyntax),
    }
}

/// Enum para representar los distintos tipos de operadores
#[derive(PartialEq, Debug)]
pub enum Operador {
    Igual,
    Mayor,
    Menor,
}
