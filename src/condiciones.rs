use crate::manejo_de_csv;
use std::io::{self, BufRead, BufReader};
use std::fs::File;
use std::error::Error;

#[derive(Debug)]
pub enum Operador {
    Igual,
    Distinto,
    Mayor,
    MayorIgual,
    Menor,
    MenorIgual
}

#[derive(Debug)]
pub enum OpLogico{
    AND,
    OR,
    NOT
}


#[derive(Debug)]
pub struct Condicion {
    columna: String ,
    operador: Operador,
    valor:String,
    op_logico:OpLogico
} 

//Mal el return..
pub fn obtener_op_logico(op:&str) -> Option<OpLogico>{

    match op {

        "AND" => Some(OpLogico::AND),
        "OR" => Some(OpLogico::OR),
        "NOT" => Some(OpLogico::NOT),

        _ => Some(OpLogico::AND)
    }

}




pub fn obtener_op(op: &str) -> Option<Operador> {

    match op {

        "=" => Some(Operador::Igual),
        "<" => Some(Operador::Menor),
        ">" => Some(Operador::Mayor),
        "!=" => Some(Operador::Distinto),
        "<=" => Some(Operador::MenorIgual),
        ">=" => Some(Operador::MayorIgual),

        _ => None
    }
}



pub fn procesar_condiciones(condiciones:Vec<String>) -> Vec<Condicion>{
  
    let mut condiciones_parseadas: Vec<Condicion> = Vec::new(); 
    println!("{:?}", condiciones);
    for i in (0..condiciones.len()).step_by(4){
        
        if let (Some(op_logico) ,Some(op), Some(val)) = (obtener_op_logico(&condiciones[i]),obtener_op(&condiciones[i + 1]), condiciones.get(i + 2)) {

            let col: &String = &condiciones[i];
            
            
            let condicion = Condicion{
                columna:col.to_string(),
                operador:op,
                valor: val.to_string(),
                op_logico:op_logico

            };
            
            condiciones_parseadas.push(condicion);
        }
        
    };

    
    condiciones_parseadas 
}


pub fn comparar_op(condicion:&Condicion,fila: &Vec<String>,pos:usize) -> Result<bool,String>{

    if let Some(valor_f) = fila.get(pos) {

        match condicion.operador{

            Operador::Igual => Ok(valor_f == &condicion.valor),
            Operador::Mayor => Ok(valor_f.parse::<i32>().ok() > condicion.valor.parse::<i32>().ok()),
            Operador::Menor => Ok(valor_f.parse::<i32>().ok() < condicion.valor.parse::<i32>().ok()),
            Operador::Distinto => Ok(valor_f != &condicion.valor),
            Operador::MayorIgual => Ok(valor_f.parse::<i32>().ok() >= condicion.valor.parse::<i32>().ok()),
            Operador::MenorIgual => Ok(valor_f.parse::<i32>().ok() <= condicion.valor.parse::<i32>().ok()),

            

        }

    }
    else {
        Err( "INVALID_COLUMN: La columna ingresada no se encuntra en el csv".to_string())
    }
}


pub fn comparar_op_logico(condiciones_parseadas:&Vec<Condicion>,fila: &Vec<String> ,header:&Vec<String>) -> Result <bool,String> {
    let mut resultado = true;
    
    for condicion in condiciones_parseadas.iter() {
       
       let pos = match manejo_de_csv::obtener_posicion_header(&condicion.columna, &header) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("{}", e); // Error al leer el header
                break;
            }
        };
     

        let segundo_resultado = match comparar_op(condicion, fila, pos) {
                Ok(segundo_resultado) => {segundo_resultado}

                Err(e) => {
                    return Err(format!("{}", e));
                }
        };

        match condicion.op_logico {
            OpLogico::AND => resultado = resultado && segundo_resultado,
            OpLogico::OR => resultado = resultado || segundo_resultado,
            OpLogico::NOT => resultado = resultado && !segundo_resultado,
        }
    }

   
    Ok(resultado)
} 


pub fn comparar_con_csv(condiciones_parseadas:Vec<Condicion>, ruta_csv: String,header:Vec<String>) ->Result<Vec<Vec<String>>, Box<dyn Error>>{
    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let mut matriz: Vec<Vec<String>> = Vec::new();

    for linea in lector.lines(){
        
        let fila: Vec<String> = linea?.split(',').map(|s| s.trim().to_string()).collect();

        let cumple_condiciones = match comparar_op_logico(&condiciones_parseadas, &fila, &header){
            Ok(segundo_resultado) => {segundo_resultado}


            Err(e) => return Err(Box::new(io::Error::new(io::ErrorKind::Other, e))),
        

        };

        if cumple_condiciones {
            matriz.push(fila);
        }
    }
    matriz.insert(0,header);

    Ok(matriz)
}
