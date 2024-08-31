//La compilación no debe arrojar warnings del compilador, ni del linter clippy.
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufWriter, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{self, Path};
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
fn separar_datos(consulta_sql:String) -> (String, String){

    let partes: Vec<&str> = consulta_sql.split("VALUES").collect();

    let insert = partes[0].trim().to_string();
    let valores = partes[1].trim().trim_end_matches(';').to_string();

    let mut palabras: Vec<&str> = insert.split_whitespace().collect();

    palabras.drain(0..2); // Quito el insert y el into

    let direccion_y_columnas = palabras.join(" ");

    (direccion_y_columnas, valores)

}
  


fn escribir_csv(ruta_csv: String, linea:&str)->io::Result<()>{ 

  
    let mut archivo = OpenOptions::new()
    .append(true)
    .open(ruta_csv)?;


    writeln!(archivo, "{}", linea)?;

    Ok(())

}


fn crear_matriz(valores:String)-> Vec<Vec<String>>{

    let valores = valores.trim_matches(|c| c == '(' || c == ')')
    .split("), (");

    let valores = valores
        .map(|fila| {
            fila
                .split(',') // Divide los valores dentro de cada tupla
                .map(|v| v.trim().trim_matches('\'').to_string()) // Limpia espacios y comillas
                .collect::<Vec<String>>() 
        })
        .collect::<Vec<Vec<String>>>();
    
    valores

}

fn obtener_ruta_del_csv(ruta:String,nombre_del_csv:&str) -> String{

    let palabras: Vec<&str> = nombre_del_csv.split(" ").collect();
    let nombre_del_csv = palabras[0];

    let ruta_de_csv = format!("{}{}{}{}",ruta,"/",nombre_del_csv,".csv");

    return ruta_de_csv.to_string();

}


fn insertar_datos(consulta_sql: String,ruta_del_archivo: String) {
    let (direccion_y_columnas,valores) = separar_datos(consulta_sql);

    let ruta = obtener_ruta_del_csv(ruta_del_archivo,&direccion_y_columnas);
    let valores = crear_matriz(valores);

    println!("{}", ruta);
    
    for (i, fila) in valores.iter().enumerate(){
       let linea = fila.join(", ");

        escribir_csv(ruta.to_string(), &linea);
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
    let consulta_sql: &String = &consulta_completa[2];  // - > consulta

 
    realizar_consulta(consulta_sql.to_string(),ruta.to_string())

}
