pub fn obtener_primera_palabra(cadena: &str) -> String {

    let mut iterar_en_cadena = cadena.split_whitespace();

    if let Some(palabra) = iterar_en_cadena.next() {

        palabra.to_string()

    }     
    else {
        String::new() 
    }
}


pub fn separar_datos(consulta_sql:String) -> (String, String){

    //Ojo que si no esta values no funciona
    let partes: Vec<&str> = consulta_sql.split("VALUES").collect();

    let insert = partes[0].trim().to_string();
    let valores = partes[1].trim_end_matches(';').trim().to_string();
 

    let mut palabras: Vec<&str> = insert.split_whitespace().collect();


    palabras.drain(0..2); // Quito el insert y el into

    let direccion_y_columnas = palabras.join(" ");

    (direccion_y_columnas, valores)

}

pub fn separar_datos_update(consulta_sql:String) -> Result<(String, Vec<String>,Vec<String>),&'static str>{

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
        None => Err("INVALID_SYNTAX: Error de sintaxis en la consulta ")
    }
    
}

pub fn separar_datos_delete(consulta_sql:String) -> Result<(String, Vec<String>),&'static str>{

    match consulta_sql.split("WHERE").collect::<Vec<&str>>(){
        
        vec if vec.len() > 1 => {

            let nombre_del_csv = vec[0].replace("DELETE","").replace("FROM", "").trim().to_string();
           
        
            let clave = vec[1].replace("=", "").replace(",","" ).trim_end_matches(";").to_string();
            let clave:Vec<String> = clave.split_whitespace().map(|s| s.to_string()).collect();
        
           
            Ok((nombre_del_csv,clave))}

        _ => Err("INVALID_SYNTAX: Error de sintaxis en la consulta "),
    }

    
}


pub fn separar_datos_select(consulta_sql: String)-> Result<(String,String ,Vec<String>),&'static str>{
    
    match consulta_sql.split("WHERE").collect::<Vec<&str>>(){
        vec if vec[0].contains("FROM") => {
        
        let nombre_csv_y_columnas = vec[0].replace("SELECT", "").trim().to_string();
        let nombre_csv_y_columnas:Vec<&str> = nombre_csv_y_columnas.split("FROM").collect();

        let nombre_csv = nombre_csv_y_columnas[1].trim().to_string();
        let columnas = nombre_csv_y_columnas[0].trim().to_string();

        

        let condiciones = vec[1].replace(";","").trim().to_string();
        let condiciones:Vec<String> = condiciones.split_whitespace().map(|s| s.to_string()).collect();

       

        Ok((nombre_csv,columnas,condiciones))}

    _ => Err("INVALID_SYNTAX: Error de sintaxis en la consulta")
    ,
    }
}

pub fn separar_order(condiciones:Vec<String>) -> (Vec<String>,Vec<String>){
    let ordenamiento:Vec<String> = Vec::new();
    let condiciones = condiciones.join(" ");

    
    if condiciones.contains("ORDER") {

       let condiciones = condiciones.split("ORDER").collect::<Vec<&str>>();
       
       let ordenamiento = condiciones[1].split_whitespace().map(|s| s.to_string()).collect();
      

       let condiciones = condiciones[0];

       let condiciones:Vec<String> = condiciones.split_whitespace().map(|s| s.to_string()).collect();

       (condiciones,ordenamiento)


    }

    else {
        let condiciones:Vec<String> = condiciones.split_whitespace().map(|s| s.to_string()).collect();
        
        (condiciones, ordenamiento)
    }
    
}


pub fn crear_matriz(valores:String)-> Vec<Vec<String>>{

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
