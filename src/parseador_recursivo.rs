use super::condiciones::Condicion;
use crate::{
    errors::SqlError,
    manejo_de_string::{parentesis_izquierdo, partentesis_derecho},
    operadores::operadores_logicos::{es_and, es_not, es_or, OpLogico},
};

/// Funcion para parsear las condiciones que llegan como tokens (tokens : [ producto, = , Monitor, And.. ] )
pub fn parsear_condicion(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condicion, SqlError> {
    let mut left = parsear_or(tokens, pos)?;

    while let Some(token) = tokens.get(*pos) {
        if es_or(token) {
            *pos += 1;
            let right = parsear_or(tokens, pos)?;
            left = Condicion::new_compleja(Some(left), OpLogico::Or, right);
        } else {
            break;
        }
    }

    Ok(left)
}

fn parsear_or(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condicion, SqlError> {
    let mut left = parsear_and(tokens, pos)?;

    while let Some(token) = tokens.get(*pos) {
        if es_and(token) {
            *pos += 1;
            let right = parsear_and(tokens, pos)?;
            left = Condicion::new_compleja(Some(left), OpLogico::And, right);
        } else {
            break;
        }
    }
    Ok(left)
}

fn parsear_and(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condicion, SqlError> {
    if let Some(token) = tokens.get(*pos) {
        if es_not(token) {
            *pos += 1;
            let expr = parsear_base(tokens, pos)?;
            Ok(Condicion::new_compleja(None, OpLogico::Not, expr))
        } else {
            parsear_base(tokens, pos)
        }
    } else {
        parsear_base(tokens, pos)
    }
}

fn parsear_base(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condicion, SqlError> {
    if let Some(token) = tokens.get(*pos) {
        if parentesis_izquierdo(token) {
            *pos += 1;
            let expr = parsear_condicion(tokens, pos)?;
            let next_token = tokens.get(*pos).ok_or(SqlError::Error)?;
            if partentesis_derecho(next_token) {
                *pos += 1;
                Ok(expr)
            } else {
                Err(SqlError::Error)
            }
        } else {
            let simple_Condicion = Condicion::new_simple_cond(tokens, pos)?;
            Ok(simple_Condicion)
        }
    } else {
        Err(SqlError::Error)
    }
}
