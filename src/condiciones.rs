use crate::manejo_de_csv;
use std::io::{self, BufRead,BufReader};
use std::fs::File;


#[derive(Debug)]
pub enum Operador {
    Igual,
    Mayor,
    Menor,
    And,
    Not,
    Or
}


#[derive(Debug)]
pub struct Condicion {
    columna: String ,
    operador: Operador,
    valor:String
} 


pub fn obtener_op(op: &str) -> Option<Operador> {

    match op {

        "=" => Some(Operador::Igual),
        "<" => Some(Operador::Menor),
        ">" => Some(Operador::Mayor),
        "AND" => Some(Operador::And),
        "OR" => Some(Operador::Or),
        "NOT" => Some(Operador::Not),


        _ => None
    }
}


pub fn procesar_condiciones(condiciones:Vec<String>) -> Vec<Condicion>{
    
    let mut condiciones_parseadas: Vec<Condicion> = Vec::new(); 
    for i in (0..condiciones.len()).step_by(4){

        if let (Some(op), Some(val)) = (obtener_op(&condiciones[i + 1]), condiciones.get(i + 2)) {

            let c = &condiciones[i];

            let condicion = Condicion{
                columna:c.to_string(),
                operador:op,
                valor: val.to_string()

            };

            condiciones_parseadas.push(condicion);
        }
         
    };

    
    condiciones_parseadas 
}


pub fn comparar_condiciones(condicion:&Condicion,fila: &Vec<String>,pos:usize) -> bool{

    if let Some(valor_f) = fila.get(pos) {

        match condicion.operador{

            Operador::Igual => valor_f == &condicion.valor,
            Operador::Mayor => valor_f.parse::<i32>().ok() > condicion.valor.parse::<i32>().ok(),
            Operador::Menor => valor_f.parse::<i32>().ok() < condicion.valor.parse::<i32>().ok(),

            _ => false

        }

    }
    else {
        false
    }
}


pub fn comparar_con_csv(condiciones_parseadas:Vec<Condicion>, ruta_csv: String,header:Vec<String>) ->Result<Vec<Vec<String>>, io::Error>{
    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let mut matriz: Vec<Vec<String>> = Vec::new();

    for linea in lector.lines(){

        let fila: Vec<String> = linea?.split(',').map(|s| s.trim().to_string()).collect();
        let mut cumple_condiciones = true;

        for condicion in &condiciones_parseadas {         

            let pos = match manejo_de_csv::obtener_posicion_header(&condicion.columna, &header) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("{} Error: No se encontro las columnas ", e); // Error de encabezado
                    break;
                }
            };

            if !comparar_condiciones(&condicion, &fila, pos){
                //Si no cumple 1 lo saco ya que no lo voy a agregar (mirar como hacer con OR y NOT)
                cumple_condiciones = false;
                break;
            }

        }

        if cumple_condiciones {
            matriz.push(fila);
        }
    }
    matriz.insert(0,header);

    Ok(matriz)
}
