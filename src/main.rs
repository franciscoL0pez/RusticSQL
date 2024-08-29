//La compilación no debe arrojar warnings del compilador, ni del linter clippy.

use std::io::{self, BufRead};
use std::path::Path;
use std::fs::File;
use std::collections::HashMap;


// Por enunciado solo recibiremos datos del tipo String o int, entonces necesito poder identificarlos, ya que el encabezado puede cambiar
#[derive(Debug)]
enum valores_del_csv {
    StringValue(String),
    IntValue(i32),
}


//En principio el valor es un string, ya que lo saco del csv. Ahora, busco transformarlo a int, en caso de no poder hacerlo lo dejo como String
fn transformar_valor(valor:&String) -> valores_del_csv{

    if let Ok(int_value) = valor.parse:: <i32>() {
        return valores_del_csv::IntValue(int_value);
    }

    else {
        valores_del_csv::StringValue(valor.to_string())
    }

}

//Creo una funcion para ver si funciona el transformar_valors
fn procesar_valor(valor: valores_del_csv) {
    match valor {
        valores_del_csv::StringValue(s) => println!("Es una cadena: {}", s),
        valores_del_csv::IntValue(i) => println!("Es un entero: {}", i),
    }
}

/* 
fn leer_archivos(archivo: &String) -> io::Result<()>{

    let path = Path::new(archivo); // Ruta del archivo  
    let file = File::open(&path)?; // Abro el archivo
    let reader = io::BufReader::new(file); // creo un reader para leer el archivo

    for linea in reader.lines() {
        println!("{}", linea?);
    }

    Ok(())
}
*/


//Por ahora leo el archivo, saco el header y atajo el error asi
fn leer_header(archivo: &String) -> io::Result<Vec<String>>{
    let path = Path::new(archivo); 
    let file = File::open(&path)?; 
    let reader = io::BufReader::new(file); 

    let mut lineas = reader.lines();

    //Leo la primera ya que quiero saber como es la estructura de mi archivo
    //Devuelvo el header o en caso de que no es
  if let Some(header_line) = lineas.next() {
      let header_line = header_line?;
      let header: Vec<String> = header_line.split(',')
                                              .map(|s| s.trim().to_string())
                                              .collect();

        Ok((header)) // Devuelve el vector de 
      
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "El archivo está vacío o no tiene encabezados"))
        }

   
}


//Es un copy paste de la funcion de arriba, pero bueno desp veo si la puedo modularizar
fn leer_registros(archivo: &String) -> io::Result<Vec<String>>{
    let path = Path::new(archivo); 
    let file = File::open(&path)?; 
    let reader = io::BufReader::new(file); 

    let mut lineas = reader.lines();

    lineas.next();// No quiero leer la primera linea ya que es el header!


    if let Some(registro) = lineas.next() {
        let registro = registro?;
        let registro: Vec<String> = registro.split(',')
                                                .map(|s| s.trim().to_string())
                                                .collect();
        
          Ok(registro) // Devuelve el vector de 
        
          } else {
              Err(io::Error::new(io::ErrorKind::NotFound, "El archivo está vacío o no tiene registros"))
          }

}


//Ahora,teniendo el header necesito saber el valor que las columnas deberia tener, necesito leer otra linea para ingresar los datos
fn obtener_valores_del_header(header: Vec<String>, registro: Vec<String>) -> io::Result<HashMap<String, valores_del_csv>>{
    let mut mapa = HashMap::new();
    
    if registro.len() == header.len(){ 

        for (columna, valor) in header.into_iter().zip(registro){       
            
                mapa.insert(columna, transformar_valor(&valor));
                
                
        }
        
        Ok(mapa)
    }

    else {

      Err(io::Error::new(io::ErrorKind::NotFound, "No coinciden las dimensiones del hashmap"))
          
}

}


fn obtener_primera_palabra(cadena: &str) -> String {

    let mut iterar_en_cadena = cadena.split_whitespace();

    if let Some(palabra) = iterar_en_cadena.next() {

        palabra.to_string()

    } else {
        String::new() // -> Si no hay palabras devuelvo una cadena sin nada
    }
}


//Me interesa separarlo en dos partes, por un lado lo que este desp del value y lo que este antes!
fn separar_datos(consulta_sql:String) -> Result<Vec<String>, &'static str>{
    let partes: Vec<&str> = consulta_sql.split(" VALUES ").collect();

    if partes.len() == 2 {
        
        let primera_parte = &partes[0]; //INsert into...
        let segunda_parte = &partes[1]; // Nuestros valores! los cuales queremos insertar!

        let valores = segunda_parte.replace("(", "");

        let valores = valores.replace(")", "");

        let columnas: Vec<&str> = primera_parte.splitn(2, " (").collect(); //Divido la cadena en 2
        let nombre_del_csv = columnas[0].replace("INSERT INTO ", ""); 

        let nombre_cols = columnas[1].replace("(", "");
        let nombre_cols = nombre_cols.replace(")", "");
        
        let mut componentes_consulta: Vec<String> = Vec::new();
        componentes_consulta.push(nombre_del_csv);
        componentes_consulta.push(nombre_cols);
        componentes_consulta.push(valores);

        Ok(componentes_consulta)
    }

    else {
        Err("Error al separar el string!")
    }
}


fn insertar_datos(consulta_sql: String,ruta_del_archivo: String) {
    let header = leer_header(&ruta_del_archivo);

    let componentes_consulta = separar_datos(consulta_sql);
   
    for i in &componentes_consulta{
        println!("{}", i);
    }
}


fn realizar_consulta(consulta_sql:String ,ruta: String) {
    
    
    if obtener_primera_palabra(&consulta_sql)== "INSERT" {

        insertar_datos(consulta_sql, ruta)
    }

    else {
        println!("Error al escribir la consulta")
    }
    
}

fn main() {
    // Simulamos lo que recibirías por la consola
    let consulta_completa: Vec<String> = std::env::args().collect();

    let ruta = &consulta_completa[1];  //  -> ruta a la carpeta de csv
    let mut consulta_sql: &String = &consulta_completa[2];  // - > consulta

 
    realizar_consulta(consulta_sql.to_string(),ruta.to_string())

}
