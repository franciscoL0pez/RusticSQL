use std::path::Path;
use std::fs::OpenOptions;
use std::fs::File;
use std::io::{self, BufRead, Write,BufReader};
use std::fs::rename;

//Por ahora leo el archivo, saco el header y atajo el error asi
pub fn leer_header(archivo: &String) ->  io::Result<Vec<String>>{
    let path = Path::new(archivo); 
    let file = File::open(path)?; 
    let reader = io::BufReader::new(file); 

    let mut lineas = reader.lines();

    //Leo la primera ya que quiero saber como es la estructura de mi archivo
    //Devuelvo el header o en caso de que no es
  if let Some(header_line) = lineas.next() {
      let header_line = header_line?;
      let header: Vec<String> = header_line.split(',')
                                              .map(|s| s.trim().to_string())
                                              .collect();

        Ok(header) // Devuelve el vector de 
      
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "CSV_Error:Error al leer el csv"))
        }

   
}


pub fn obtener_ruta_del_csv(ruta:String,nombre_del_csv:&str) -> String{

    let palabras: Vec<&str> = nombre_del_csv.split(" ").collect();
    let nombre_del_csv = palabras[0];

    let ruta_de_csv = format!("{}{}{}{}",ruta,"/",nombre_del_csv,".csv");

    ruta_de_csv.to_string()

}


pub fn escribir_csv(ruta_csv: String, linea:&str)->io::Result<()>{ 
  
    let mut archivo = OpenOptions::new()
    .append(true)
    .open(ruta_csv)?;


    writeln!(archivo, "{}", linea)?;

    Ok(())

}

pub //Achicar la funcion!
fn actualizar_csv(ruta_csv:String,header:Vec<String>,campos:Vec<String>,clave:Vec<String>)-> io::Result<()>{

    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let archivo_temporal = "auxiliar.csv";
    let mut archivo_tem = File::create(archivo_temporal)?;


    let pos = header.iter().position(|s| *s == clave[0].to_string());
    
    let indice = match pos {

        Some(i) => i,

        None => {
          
            return Err(io::Error::new(io::ErrorKind::NotFound, "INVALID_COLUMN: Error al buscar las columnas en la consulta"));
        },
        
    }; 

    //Quiero encontrar la clave en alguna linea y si la encuentro la reemplazo por los valores que me dieron
    for linea in lector.lines(){
        let mut linea_csv: Vec<String> = linea?.split(',').map(|s| s.trim().to_string()) .collect();
        
        //Si el valor de la clave coicide, encontre el elemento que quiero cambiar
        if clave[1] == linea_csv[indice] {

            for (i,valor_para_act) in campos.iter().enumerate(){

                 for (j, val_header) in header.iter().enumerate() {

                    if valor_para_act == val_header { linea_csv[j] = campos[i+1].to_string(); }

                 }  
            
            }
            
            let nueva_linea = linea_csv.join(",");
            writeln!(archivo_tem,"{}",nueva_linea)?;

        } else {
            let linea = linea_csv.join(",");
            let _ = writeln!(archivo_tem,"{}",linea);    
                
            }
    }
    
    let _ = rename(archivo_temporal,ruta_csv);

    Ok(())
}


pub fn borrar_lineas_csv(ruta_csv:String,header:Vec<String>,clave:Vec<String>)-> io::Result<()>{
    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let archivo_temporal = "auxiliar.csv";
    let mut archivo_tem = File::create(archivo_temporal)?;
    
    let pos = header.iter().position(|s| *s == clave[0].to_string());
    
    let indice = match pos {

        Some(i) => i,

        None => {
            return Err(io::Error::new(io::ErrorKind::NotFound, "INVALID_COLUMN: Error al buscar las columnas en la consulta"));
        },
        
    }; 
    

    for linea in lector.lines(){
        let  linea_csv: Vec<String> = linea?.split(',').map(|s| s.trim().to_string()) .collect();
        
        //Si el valor de la clave coicide, encontre el elemento que quiero eliminar
        if clave[1] != linea_csv[indice] {

            let nueva_linea = linea_csv.join(",");
            writeln!(archivo_tem,"{}",nueva_linea)?;
        }         

      
    }   
    

    let _ = rename(archivo_temporal,ruta_csv);
    Ok(())
    
}

pub fn obtener_posicion_header(clave:&str, header:&Vec<String>) -> Result<usize,String>{

    let pos = header.iter().position(|s| *s == clave.to_string());
    
    let _i = match pos {

        Some(i) => return Ok(i),

        None => {
            return Err( "INVALID_COLUMN: La columna ingresada no se encuntra en el csv".to_string());
        },
        
    }; 
   
}