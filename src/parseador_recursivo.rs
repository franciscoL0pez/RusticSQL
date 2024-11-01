use super::condiciones::Condicion;
use crate::{
    errors::SqlError,
    operadores::operadores_logicos::{OpLogico,is_and,is_not,is_or},
    manejo_de_string::{is_left_paren,is_right_paren},

};

pub fn parsear_condicion(tokens: &Vec<&str>, pos: &mut usize) -> Result<Condicion, SqlError> {
    let mut left = parsear_or(tokens, pos)?;

    while let Some(token) = tokens.get(*pos) {
        if is_or(token) {
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
        if is_and(token) {
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
        if is_not(token) {
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
        if is_left_paren(token) {
            *pos += 1;
            let expr = parsear_condicion(tokens, pos)?;
            let next_token = tokens.get(*pos).ok_or(SqlError::Error)?;
            if is_right_paren(next_token) {
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