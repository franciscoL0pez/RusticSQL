//La compilación no debe arrojar warnings del compilador, ni del linter clippy.
use std::fs::OpenOptions;
use std::io::{self, BufRead, Write,BufReader};
use std::path::Path;
use std::fs::File;
use std::fs::rename;

//Por ahora leo el archivo, saco el header y atajo el error asi
fn leer_header(archivo: &String) ->  io::Result<Vec<String>>{
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
            Err(io::Error::new(io::ErrorKind::NotFound, "Error al leer el archivo"))
        }

   
}

fn obtener_primera_palabra(cadena: &str) -> String {

    let mut iterar_en_cadena = cadena.split_whitespace();

    if let Some(palabra) = iterar_en_cadena.next() {

        palabra.to_string()

    } else {
        String::new() 
    }
}


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

    ruta_de_csv.to_string()

}


fn insertar_datos(consulta_sql: String,ruta_del_archivo: String) {
    let (direccion_y_columnas,valores) = separar_datos(consulta_sql);

    let ruta = obtener_ruta_del_csv(ruta_del_archivo,&direccion_y_columnas);
    let valores = crear_matriz(valores);

    
    for fila in valores.iter(){
       let linea = fila.join(", ");

        let _ = escribir_csv(ruta.to_string(), &linea);
    }
    

}


fn separar_datos_update(consulta_sql:String) -> Result<(String, Vec<String>,Vec<String>),&'static str>{

    let partes: Vec<&str> = consulta_sql.split("SET").collect();
    let nombre_del_csv = partes[0].trim().replace("UPDATE","").replace(" ", "");
    let valores = partes[1].trim().trim_end_matches(';');

    match valores.split_once("WHERE"){
        
        Some((campos,clave)) => {

            let campos = campos.replace("=", "").replace(",","" );
            let campos:Vec<String> = campos.split_whitespace().map(|s| s.to_string()).collect();
        
            let clave = clave.replace("=", "").replace(",","" );
            let clave:Vec<String> = clave.split_whitespace().map(|s| s.to_string()).collect();
        
            
            Ok((nombre_del_csv,campos, clave))}
        None => Err("Error al escribir la consulta")
    }
    
}

fn actualizar_csv(ruta_csv:String,header:Vec<String>,campos:Vec<String>,clave:Vec<String>)-> io::Result<()>{

    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let archivo_temporal = "auxiliar.csv";
    
    let mut archivo_tem = File::create(archivo_temporal)?;



    let pos = header.iter().position(|s| *s == clave[0].to_string());
    
    let indice = match pos {

        Some(i) => i,

        None => {
            println!("Error no existe esa clave!");
            return Err(io::Error::new(io::ErrorKind::NotFound, "Error no existe esa clave!"));
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


fn actualizar_datos(consulta_sql: String, ruta_del_archivo: String){

    let (nombre_del_csv,campos_para_actualizar, donde_actualizar) =  match separar_datos_update(consulta_sql) {
        Ok((nombre_del_csv,campos_para_actualizar,donde_actualizar)) => {(nombre_del_csv,campos_para_actualizar, donde_actualizar)}

        Err(e) => {println!("Error: {}", e);
        return; },
        
    };
   
    let ruta_csv = obtener_ruta_del_csv(ruta_del_archivo,&nombre_del_csv);

    let header = match leer_header(&ruta_csv) {
        Ok(header) => {header}

        Err(e) => {println!("Error: {}", e);
        return;}, 
    };
    

    let _ = actualizar_csv(ruta_csv, header,campos_para_actualizar,donde_actualizar);
    

    }




fn realizar_consulta(consulta_sql:String ,ruta: String) {
    
    
    if obtener_primera_palabra(&consulta_sql)== "INSERT" {

        insertar_datos(consulta_sql, ruta)
    }

    else if obtener_primera_palabra(&consulta_sql) == "UPDATE"  {

        actualizar_datos(consulta_sql,ruta)
    }


    else {
        println!("No existe la consulta escrita!")
    }
    
}


fn main() {
    // Simulamos lo que recibirías por la consola
    let consulta_completa: Vec<String> = std::env::args().collect();

    let ruta = &consulta_completa[1];  //  -> ruta a la carpeta de csv
    let consulta_sql: &String = &consulta_completa[2];  // - > consulta

   
    realizar_consulta(consulta_sql.to_string(),ruta.to_string())

}
