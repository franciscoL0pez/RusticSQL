///Funcion para obtener la primera palabra de nuestra consulta
///#Recibe una cadena por parametro con la consulta completa
///-Divide la cadena por espacios
///-Devuelve la primera palabra encontrada
///-En otro caso devuelve un string vacio (no hice que devuelva un error por que se maneja en el main)
pub fn obtener_primera_palabra(cadena: &str) -> String {
    let mut iterar_en_cadena = cadena.split_whitespace();

    if let Some(palabra) = iterar_en_cadena.next() {
        palabra.to_string()
    } else {
        String::new()
    }
}

///Funcion para separar los datos de la consulta INSERT
///#Recibe por parametro la consulta
///-Define un vector con dos partes, usando VALUES para separar
///-Luego separa esas dos partes y opera para dejar valores y direccione_y_columnas como Strings separados
///-Finalmente retorna los dos Strings
pub fn separar_datos(consulta_sql: String) -> Result<(String, String), &'static str> {
    let palabras: Vec<&str> = consulta_sql.split_whitespace().collect();

     if let Some(pos) = palabras.iter().position(|&x| x == "VALUES") { 

         if palabras[..pos].join(" ").contains("INTO") {

                let insert = palabras[..pos].join(" ").to_string();
                let valores = palabras[pos + 1..].join(" ").trim_end_matches(';').trim().to_string();

                let mut columnas: Vec<&str> = insert.split_whitespace().collect();
                columnas.drain(0..2); // Quitamos "INSERT" y "INTO"
    
                let direccion_y_columnas = columnas.join(" ");
            


            Ok((direccion_y_columnas, valores))
        } else {
            Err("INVALID_SYNTAX: Error de sintaxis en la consulta ")
        }
    } else {
        Err("INVALID_SYNTAX: Error de sintaxis en la consulta ")
    }
}

///Funcion para separar los datos de la consulta UPDATE
///#Recibe por parametro la consulta sql
///-Divide la consulta en dos partes una con el nombre del csv y otra con los valores
///-Con la primera cadena que contiene el nombre reemplaza el UPDATE para dejar solo el nombre del csv
///-Con la segunda cadena que contiene los valores itera sobre dicha cadena y separa la clave de los campos a actualizar
///-Finalmente retorn un string con el nombre, un vector con los valores y otro con la clave para actualizar
///-En otro caso devuelve un error
pub fn separar_datos_update(
    consulta_sql: String,
) -> Result<(String, Vec<String>, Vec<String>), &'static str> {
    let partes: Vec<&str> = consulta_sql.split("SET").collect();
    let nombre_del_csv = partes[0].trim().replace("UPDATE", "").replace(" ", "");
    let valores = partes[1].trim().trim_end_matches(';');

    match valores.split_once("WHERE") {
        Some((campos, clave)) => {
            let campos = campos.replace("=", "").replace(",", "");
            let campos: Vec<String> = campos.split_whitespace().map(|s| s.to_string()).collect();

            let clave = clave.replace("=", "").replace(",", "");
            let clave: Vec<String> = clave.split_whitespace().map(|s| s.to_string()).collect();

            Ok((nombre_del_csv, campos, clave))
        }
        None => Err("INVALID_SYNTAX: Error de sintaxis en la consulta "),
    }
}

///Funcion para separar los datos de la consulta DELETE
///#Recibe por parametro la consulta sql
///-Divide la consulta en dos partes una con el nombre del csv y otra con la clave
///-Con la primera cadena que contiene el nombre reemplaza el DELETE  y el FROM para dejar solo el nombre del csv
///-Con la segunda cadena que contiene los valores itera sobre dicha cadena y deja solamente la calve y el valor a actualizar
///-Finalmente retorn un string con el nombre y un vector clave-valor
///-En otro caso devuelve un error
pub fn separar_datos_delete(consulta_sql: String) -> Result<(String, Vec<String>), &'static str> {
    match consulta_sql.split("WHERE").collect::<Vec<&str>>() {
        vec if vec.len() > 1 => {
            let nombre_del_csv = vec[0]
                .replace("DELETE", "")
                .replace("FROM", "")
                .trim()
                .to_string();

            let clave = vec[1]
                .replace("=", "")
                .replace(",", "")
                .trim_end_matches(";")
                .to_string();
            let clave: Vec<String> = clave.split_whitespace().map(|s| s.to_string()).collect();

            Ok((nombre_del_csv, clave))
        }

        _ => Err("INVALID_SYNTAX: Error de sintaxis en la consulta "),
    }
}

///Funcion para separar los datos de la consulta SELECT
///#Recibe por parametro la consulta sql
///-Divide la consulta en dos partes una con el nombre + columnas y otra con la clave
///-Con la primera cadena que contiene el nombre reemplaza el SELECT  y el FROM para dejar solo el nombre del csv
/// Y en otra variable las columnas del SELECT
///-Con la segunda cadena que contiene las condiciones quita los ; y recoge todo en un vector.
///-Finalmente retorn un string con el nombre otro con las columnas y por ultimo un vector con las condiciones
///-En otro caso devuelve un error
pub fn separar_datos_select(
    consulta_sql: String,
) -> Result<(String, String, Vec<String>), &'static str> {
    match consulta_sql.split("WHERE").collect::<Vec<&str>>() {
        vec if vec[0].contains("FROM") => {
            let nombre_csv_y_columnas = vec[0].replace("SELECT", "").trim().to_string();
            let nombre_csv_y_columnas: Vec<&str> = nombre_csv_y_columnas.split("FROM").collect();

            let nombre_csv = nombre_csv_y_columnas[1].trim().to_string();
            let columnas = nombre_csv_y_columnas[0].trim().to_string();

            let condiciones = vec[1].replace(";", "").trim().to_string();
            let condiciones: Vec<String> = condiciones
                .split_whitespace()
                .map(|s| s.to_string())
                .collect();

            Ok((nombre_csv, columnas, condiciones))
        }

        _ => Err("INVALID_SYNTAX: Error de sintaxis en la consulta"),
    }
}

///Funcion para separar el ORDER de las condiciones de un SELECT
///#Recibe por parametro un vector con las condiciones de la consulta
///-Si contiene order itera sobre la cadena y quita el order para almacernar el resto de los strings en un vector
///-Separa el vector en dos strings y luego los mapea en dos vectores que contengan las condiciones y por otro lado el ORDER
///-Finalmente devuelve dos vectores uno con el criterio de ordenamiento y otro con las condiciones
///-En caso de no contener ORDER devuelve un vector con las condiciones y uno de ordenamiento vacio.
pub fn separar_order(condiciones: Vec<String>) -> (Vec<String>, Vec<String>) {
    let ordenamiento: Vec<String> = Vec::new();
    let condiciones = condiciones.join(" ");

    if condiciones.contains("ORDER") {
        let condiciones = condiciones.split("ORDER").collect::<Vec<&str>>();

        let ordenamiento = condiciones[1]
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        let condiciones = condiciones[0];

        let condiciones: Vec<String> = condiciones
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        (condiciones, ordenamiento)
    } else {
        let condiciones: Vec<String> = condiciones
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();

        (condiciones, ordenamiento)
    }
}

///Funcion para crear una matriz a la hora de utilizar el INSERT con multiples valores
///#Recibe por parametro el String con los valores a insertar
///-Realizar un trim de limitado por parentesis y por las comas
///-Luego mapea los valores y los pone en un vector
///-Finalmente los devuelve en formato de matriz
///#Ejemplo:
///-Recibo por parametro (1,2,Monitor,22), (1,3,Monitor,0) (mis valores a insertar)
///-Elimina los parentesis y realiza realiza un split con las comas y va juntando los valores como vector
///-Luego añade los vectores a un vector de vectores para formar una matriz
///-Finalmente retorna la matriz creada
pub fn crear_matriz(valores: String) -> Vec<Vec<String>> {
    let valores = valores.trim_matches(|c| c == '(' || c == ')').split("), (");

    let valores = valores
        .map(|fila| {
            fila.split(',') // Divide los valores dentro de cada tupla
                .map(|v| v.trim().trim_matches('\'').to_string()) // Limpia espacios y comillas
                .collect::<Vec<String>>()
        })
        .collect::<Vec<Vec<String>>>();

    valores
}

#[cfg(test)]
mod test {

    use std::vec;

    use super::*;

    #[test]
    fn test05devuelve_la_primera_palabra_de_una_consulta() {
        let consulta = "UPDATE ordenes SET producto = cangrejo WHERE producto = Altavoces ";

        let primera_palabra = obtener_primera_palabra(&consulta);

        let palabra_esperada = "UPDATE";
        assert_eq!(primera_palabra, palabra_esperada);
    }
    //El resto de las funciones para separar son muy parecidas
    #[test]
    fn test06separa_los_datos_del_select_y_los_devuelve() {
        let consulta = "SELECT id,producto,cantidad FROM ordenes WHERE producto = Teclado AND cantidad >= 1 ORDER BY CANTIDAD ASC ".to_string();

        let (nombre_csv, columnas, condiciones) = separar_datos_select(consulta).unwrap();

        let nombre_csv_esperado = "ordenes";
        let columnas_eperadas = "id,producto,cantidad";
        let condiciones_esperadas = vec![
            "producto".to_string(),
            "=".to_string(),
            "Teclado".to_string(),
            "AND".to_string(),
            "cantidad".to_string(),
            ">=".to_string(),
            "1".to_string(),
            "ORDER".to_string(),
            "BY".to_string(),
            "CANTIDAD".to_string(),
            "ASC".to_string(),
        ];

        assert_eq!(nombre_csv, nombre_csv_esperado);
        assert_eq!(columnas, columnas_eperadas);
        assert_eq!(condiciones, condiciones_esperadas);
    }
}
