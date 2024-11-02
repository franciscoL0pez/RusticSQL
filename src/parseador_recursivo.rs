use super::condiciones::Condicion;
use crate::{
    errors::SqlError,
    manejo_de_string::{parentesis_izquierdo, partentesis_derecho},
    operadores::operadores_logicos::{es_and, es_not, es_or, OpLogico},
};

/// Funcion para parsear las condiciones que llegan como tokens (tokens : [ producto, = , Monitor, And.. ] )
/// # Recibe un vector de tokens y un puntero con una referencia mutable para la pos
/// - Se encarga de parsear las condiciones de la consulta
/// - Puede devolver una condicion simple o compleja dependiendo de los tokens que reciba
/// - Retorna una Condicion o un Error en caso de que falle
///  (Dejo los tests como ejemplos) 
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
            let simple_condicion = Condicion::new_simple_cond(tokens, pos)?;
            Ok(simple_condicion)
        }
    } else {
        Err(SqlError::Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operadores::operadores_logicos::OpLogico;

    #[test]
    fn parseo_una_condicion_simple() {
        //El operador podria ser >, <, = 
        let tokens = vec!["producto", "=", "Monitor"];
        let mut pos = 0;
        let condicion = parsear_condicion(&tokens, &mut pos).unwrap();
        assert_eq!(
            condicion,
               Condicion::new_simple("producto", "=", "Monitor").unwrap()
            
        );
    }

    #[test]
    fn parseo_una_condicion_con_or() {
        let tokens = vec!["producto", "=", "Monitor", "OR", "precio", ">", "100"];
        let mut pos = 0;
        let condicion = parsear_condicion(&tokens, &mut pos).unwrap();
        assert_eq!(
            condicion,
            Condicion::new_compleja(
                Some(Condicion::new_simple("producto", "=", "Monitor").unwrap()),
                OpLogico::Or,
                Condicion::new_simple("precio", ">", "100").unwrap()
            )
        );
    }

    #[test]
    fn parseo_una_condicion_con_and_y_or() {
        let tokens = vec![
            "producto", "=", "Monitor", "AND", "precio", ">", "100", "OR", "marca", "=", "LG",
        ];
        let mut pos = 0;
        let condicion = parsear_condicion(&tokens, &mut pos).unwrap();
        assert_eq!(
            condicion,
            Condicion::new_compleja(
                Some(Condicion::new_compleja(
                    Some(Condicion::new_simple("producto", "=", "Monitor").unwrap()),
                    OpLogico::And,
                    Condicion::new_simple("precio", ">", "100").unwrap()
                )),
                OpLogico::Or,
                Condicion::new_simple("marca", "=", "LG").unwrap()
            )
        );
    }

    #[test]
fn parseo_un_condicion_con_not_and_y_or() {
    let tokens = vec![
        "NOT", "(", "producto", "=", "Monitor", "AND", "precio", ">", "100", ")", "OR", "marca", "=", "LG",
    ];
    let mut pos = 0;
    let condicion = parsear_condicion(&tokens, &mut pos).unwrap();

    assert_eq!(
        condicion,
        Condicion::new_compleja(
            Some(Condicion::new_compleja(
                None,
                OpLogico::Not,
                Condicion::new_compleja(
                    Some(Condicion::new_simple("producto", "=", "Monitor").unwrap()),
                    OpLogico::And,
                    Condicion::new_simple("precio", ">", "100").unwrap()
                )
            )),
            OpLogico::Or,
            Condicion::new_simple("marca", "=", "LG").unwrap()
        )
    );
    }

}