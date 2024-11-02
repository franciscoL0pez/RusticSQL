use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::errors::{self, SqlError};

use crate::operadores::operador::Operador;
use crate::operadores::operadores_logicos::OpLogico;
///Estruct para poder guardar las condiciones (simple o compleja)
#[derive(Debug, PartialEq)]
pub enum Condicion {
    Simple {
        campo: String,
        operadores: Operador,
        valor: String,
    },
    Compleja {
        izquierda: Option<Box<Condicion>>, //Hago este caso para cuando tengo un not
        operadores: OpLogico,
        derecha: Box<Condicion>,
    },
}

impl Condicion {
    pub fn new_simple_cond(tokens: &[&str], pos: &mut usize) -> Result<Self, SqlError> {
        if let Some(campo) = tokens.get(*pos) {
            *pos += 1;

            if let Some(operadores) = tokens.get(*pos) {
                *pos += 1;

                if let Some(valor) = tokens.get(*pos) {
                    *pos += 1;
                    Ok(Condicion::new_simple(campo, operadores, valor)?)
                } else {
                    Err(SqlError::InvalidSyntax)
                }
            } else {
                Err(SqlError::InvalidSyntax)
            }
        } else {
            Err(SqlError::InvalidSyntax)
        }
    }

    pub fn new_simple(campo: &str, operadores: &str, valor: &str) -> Result<Self, SqlError> {
        let op = match operadores {
            "=" => Operador::Igual,
            ">" => Operador::Mayor,
            "<" => Operador::Menor,
            _ => return Err(SqlError::InvalidSyntax),
        };

        Ok(Condicion::Simple {
            campo: campo.to_string(),
            operadores: op,
            valor: valor.to_string(),
        })
    }

    pub fn new_compleja(
        izquierda: Option<Condicion>,
        operadores: OpLogico,
        derecha: Condicion,
    ) -> Self {
        Condicion::Compleja {
            izquierda: izquierda.map(Box::new),
            operadores,
            derecha: Box::new(derecha),
        }
    }

    pub fn execute(&self, fila: &HashMap<String, String>) -> Result<bool, SqlError> {
        let op_result: Result<bool, SqlError> = match &self {
            Condicion::Simple {
                campo,
                operadores,
                valor,
            } => {
                let y = valor;
                if let Some(x) = fila.get(campo) {
                    match operadores {
                        Operador::Menor => Ok(x < y),
                        Operador::Mayor => Ok(x > y),
                        Operador::Igual => Ok(x == y),
                    }
                } else {
                    Err(SqlError::Error)
                }
            }
            Condicion::Compleja {
                izquierda,
                operadores,
                derecha,
            } => match operadores {
                OpLogico::Not => {
                    let result = derecha.execute(fila)?;
                    Ok(!result)
                }
                OpLogico::Or => {
                    if let Some(izquierda) = izquierda {
                        let izquierda_result = izquierda.execute(fila)?;
                        let derecha_result = derecha.execute(fila)?;
                        Ok(izquierda_result || derecha_result)
                    } else {
                        Err(SqlError::Error)
                    }
                }
                OpLogico::And => {
                    if let Some(izquierda) = izquierda {
                        let izquierda_result = izquierda.execute(fila)?;
                        let derecha_result = derecha.execute(fila)?;
                        Ok(izquierda_result && derecha_result)
                    } else {
                        Err(SqlError::Error)
                    }
                }
            },
        };
        op_result
    }
}

///Funcion para comparar las lineas del csv y ver si cumplen las condiciones ingresadas
///#Recibe por parametro un vector con las condiciones parseadas, la ruta del csv y el header
///-Itera sobre el csv y lo lee linea por linea
///-Compara si la linea del csv cumple con las condiciones que llegan por parametro
///-Si cumple las condiciones lo pushea a una matriz
///-Repite el proceso hasta terminar de recorrer el csv
///-Finalmente retorna una matriz con las lineas que cumplan con las condiciones
pub fn comparar_con_csv(
    condiciones_parseadas: Condicion,
    ruta_csv: String,
    header: &[String],
) -> Result<Vec<Vec<String>>, SqlError> {
    let archivo = match File::open(&ruta_csv) {
        Ok(archivo) => archivo,
        Err(_) => {
            return Err(errors::SqlError::Error);
        }
    };

    let lector = BufReader::new(archivo);
    let mut matriz: Vec<Vec<String>> = Vec::new();

    for (index, linea) in lector.lines().enumerate() {
        //Salteo la primera linea para no leer el header
        if index == 0 {
            continue;
        }
        let linea = match linea {
            Ok(linea) => linea,
            Err(_) => {
                return Err(errors::SqlError::Error);
            }
        };

        //Utilizo la linea que me devuelve el lector
        //convierto la linea en un hasmap para poder comparar
        //Utilizo el header tal que el hasmap sea columna:valor tiene que estar primero el valor del header y luego el de la linea
        let fila: HashMap<String, String> = header
            .iter()
            .zip(linea.split(',').map(|s| s.trim().to_string()))
            .map(|(a, b)| (a.to_string(), b))
            .collect();

        let cumple_condiciones = match condiciones_parseadas.execute(&fila) {
            Ok(resultado) => resultado,

            Err(e) => return Err(e),
        };

        if cumple_condiciones {
            let fila: Vec<String> = linea.split(',').map(|s| s.trim().to_string()).collect();

            matriz.push(fila);
        }
    }
    matriz.insert(0, header.to_owned());

    Ok(matriz)
}
