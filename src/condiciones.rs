use crate::manejo_de_csv;
use std::error::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

/// Enum para representar los distintos tipos de operadores
#[derive(Debug)]
pub enum Operador {
    Igual,
    Distinto,
    Mayor,
    MayorIgual,
    Menor,
    MenorIgual,
}

///Enum para representar los distintos tipos de operadores logicos
#[derive(Debug)]
pub enum OpLogico {
    And,
    Or,
    Not,
}

///Estruct para poder guardar las condiciones separandola en datos
#[derive(Debug)]
pub struct Condicion {
    columna: String,
    operador: Operador,
    valor: String,
    op_logico: OpLogico,
}

//Funcion para obtener el tipo de operador logico
pub fn obtener_op_logico(op: &str) -> Option<OpLogico> {
    match op {
        "AND" => Some(OpLogico::And),
        "OR" => Some(OpLogico::Or),
        "NOT" => Some(OpLogico::Not),

        _ => Some(OpLogico::And),
    }
}

///Funcion para obtener el tipo de operador
pub fn obtener_op(op: &str) -> Option<Operador> {
    match op {
        "=" => Some(Operador::Igual),
        "<" => Some(Operador::Menor),
        ">" => Some(Operador::Mayor),
        "!=" => Some(Operador::Distinto),
        "<=" => Some(Operador::MenorIgual),
        ">=" => Some(Operador::MayorIgual),

        _ => None,
    }
}

///Funcion para procesar los distintos tipos de condiciones que pueden llegar dependiendo de la consulta
/// -Ejemplo
/// #Recibe un vector de condiciones como por ejemplo: ["producto", "=", "Teclado", "AND", "cantidad", ">=", 1]
/// -Itera sobre los elementos de este vector y va rellenando un strut "condicion"
/// -Luego pushea la nueva condicion en un vector de condiciones parseadas
/// -Repite e itera hasta llegar al final de la lista
/// -Finalmente retorna el nuevo vector con las condiciones "parseadas"

pub fn procesar_condiciones(condiciones: Vec<String>) -> Vec<Condicion> {
    let mut condiciones_parseadas: Vec<Condicion> = Vec::new();
    let mut i = 0;

    while condiciones.len() > i {
        let mut op_logico = OpLogico::And;

        let columna = condiciones[i].to_string();
        let operador = match obtener_op(&condiciones[i + 1]) {
            Some(operador) => operador,
            None => {
                eprintln!("Operador inválido en la condición: {}", condiciones[i + 1]);
                break;
            }
        };
        let valor = condiciones[i + 2].to_string();

        if i > 1 && condiciones.len() > i {
            if let Some(op) = obtener_op_logico(&condiciones[i - 1].to_uppercase()) {
                op_logico = op;
            }
        }

        i += 4;
        let condicion = Condicion {
            columna,
            operador,
            valor,
            op_logico,
        };

        condiciones_parseadas.push(condicion);
    }
    condiciones_parseadas
}

///Funcion para comparar las operacion que tiene cada condicion
/// #Recibe por parametro la condicion, la fila y la posicion
/// -Itera sobre el operador de dicha condicion
/// -Realiza la operacion entre el valor de la fila del csv y nuestra condicion
/// -Retorna si es verdadero
pub fn comparar_op(condicion: &Condicion, fila: &[String], pos: usize) -> Result<bool, String> {
    if let Some(valor_f) = fila.get(pos) {
        match condicion.operador {
            Operador::Igual => Ok(valor_f == &condicion.valor),
            Operador::Mayor => {
                Ok(valor_f.parse::<i32>().ok() > condicion.valor.parse::<i32>().ok())
            }
            Operador::Menor => {
                Ok(valor_f.parse::<i32>().ok() < condicion.valor.parse::<i32>().ok())
            }
            Operador::Distinto => Ok(valor_f != &condicion.valor),
            Operador::MayorIgual => {
                Ok(valor_f.parse::<i32>().ok() >= condicion.valor.parse::<i32>().ok())
            }
            Operador::MenorIgual => {
                Ok(valor_f.parse::<i32>().ok() <= condicion.valor.parse::<i32>().ok())
            }
        }
    } else {
        Err("INVALID_SYNTAX: El operador ingresado no existe".to_string())
    }
}

///Funcion para comparar utilizando el operador logico que recibe de la consulta
/// #Recibe por parametro las condiciones parseadas, la fila y el header
/// -Revia que la columna de la condicion exista y devuelve su posicion
/// -Compara la OP para observar si es verdadera o falsa
/// -Vuelve a comparar pero usando nuestro operador logico para tener un resultado final
/// -Devuelve el resulta final luego de realizar las comparaciones
pub fn comparar_op_logico(
    condiciones_parseadas: &[Condicion],
    fila: &[String],
    header: &[String],
) -> Result<bool, String> {
    let mut resultado = true;

    for condicion in condiciones_parseadas.iter() {
        let pos = match manejo_de_csv::obtener_posicion_header(&condicion.columna, header) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("{}", e); // Error al leer el header
                break;
            }
        };

        let segundo_resultado = match comparar_op(condicion, fila, pos) {
            Ok(segundo_resultado) => segundo_resultado,

            Err(e) => {
                return Err(e.to_string());
            }
        };

        match condicion.op_logico {
            OpLogico::And => resultado = resultado && segundo_resultado,
            OpLogico::Or => resultado = resultado || segundo_resultado,
            OpLogico::Not => resultado = resultado && !segundo_resultado,

           
        }
    }

    Ok(resultado)
}
///Funcion para comparar las lineas del csv y ver si cumplen las condiciones ingresadas
///#Recibe por parametro un vector con las condiciones parseadas, la ruta del csv y el header
///-Itera sobre el csv y lo lee linea por linea
///-Compara si la linea del csv cumple con las condiciones que llegan por parametro
///-Si cumple las condiciones lo pushea a una matriz
///-Repite el proceso hasta terminar de recorrer el csv
///-Finalmente retorna una matriz con las lineas que cumplan con las condiciones
pub fn comparar_con_csv(
    condiciones_parseadas: Vec<Condicion>,
    ruta_csv: String,
    header: Vec<String>,
) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let mut matriz: Vec<Vec<String>> = Vec::new();

    for linea in lector.lines() {
        let fila: Vec<String> = linea?.split(',').map(|s| s.trim().to_string()).collect();

        let cumple_condiciones = match comparar_op_logico(&condiciones_parseadas, &fila, &header) {
            Ok(segundo_resultado) => segundo_resultado,

            Err(e) => return Err(Box::new(io::Error::new(io::ErrorKind::Other, e))),
        };

        if cumple_condiciones {
            matriz.push(fila);
        }
    }
    matriz.insert(0, header);

    Ok(matriz)
}
