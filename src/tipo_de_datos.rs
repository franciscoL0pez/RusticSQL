use crate::{
    errors::{self, SqlError},
    manejo_de_csv,
};

/// Enum para representar los distintos tipos de datos
/// - String: Para representar un string
/// - Interger: Para representar un numero
#[derive(Debug)]
pub enum Dato {
    String(String),
    Interger(i64),
}
/// ---Asumo que siempre el csv tiene 1 lienea ademas del header para poder saber los tipos de datos---
/// Funcion que se encarga de convertir los valores de la 2 linea de mi csv a datos
/// # Recibe por parametro la ruta del csv
/// - Leo la 2 linea de mi csv
/// - Convierto los valores a datos
/// - Devuelvo los datos convertidos
/// - Si no se puede convertir devuelvo un error
pub fn convertir_strings_a_datos_csv(ruta_csv: &String) -> Result<Vec<Dato>, SqlError> {
    let registro = match manejo_de_csv::leer_header(&ruta_csv, 1) {
        Ok(registro) => registro,

        Err(_e) => {
            return Err(errors::SqlError::Error);
        }
    };

    let mut tipo_de_datos_csv: Vec<Dato> = Vec::new();
    for valor in registro {
        if let Ok(numero) = valor.parse::<i64>() {
            tipo_de_datos_csv.push(Dato::Interger(numero));
        } else {
            tipo_de_datos_csv.push(Dato::String(valor));
        }
    }

    Ok(tipo_de_datos_csv)
}

/// Funcion que se encarga de convertir los valores a datos
/// # Recibe por parametro el valor que se quiere convertir
/// - Si el valor es un numero lo convierto a Interger
/// - Si no lo convierto a String
/// - Devuelvo el valor convertido
/// - Si no se puede convertir devuelvo un error
pub fn convertir_a_dato(valor: &str) -> Dato {
    if let Ok(numero) = valor.parse::<i64>() {
        Dato::Interger(numero)
    } else {
        Dato::String(valor.to_string())
    }
}
/// Funcion que se encarga de comparar los datos
/// # Recibe por parametro el dato que se quiere insertar y el dato que esta en el csv
/// Comparo los datos
/// Si son iguales devuelvo true
/// Si no devuelvo false
pub fn comparar_datos(dato_consulta: &Dato, dato_csv: &Dato) -> bool {
    match (dato_consulta, dato_csv) {
        (Dato::Interger(_), Dato::Interger(_)) => true,
        (Dato::String(_), Dato::String(_)) => true,
        _ => false,
    }
}

/// Funcion que se encarga de comprobar si el dato que se quiere insertar es del mismo tipo que el que esta en el csv
/// # Recibe por parametro el dato que se quiere insertar, la ruta del csv y la posicion de la columna
/// - Convierte los valores de la 2 linea de mi csv a datos
/// - Convierte el dato que se quiere insertar a dato
/// - Compara los datos
/// - Devuelve el valor del dato que se quiere insertar
/// - Si no se cumple la condicion devuelve un error
/// - Si el dato a insertar es vacio devuelve el dato a insertar
pub fn comprobar_dato(
    dato_consulta: &String,
    ruta_csv: &String,
    pos_col: usize,
) -> Result<String, SqlError> {
    if dato_consulta != "" {
        let datos_csv = match convertir_strings_a_datos_csv(ruta_csv) {
            Ok(datos_csv) => datos_csv,

            Err(e) => return Err(e),
        };

        let dato_consulta = convertir_a_dato(&dato_consulta);

        if comparar_datos(&dato_consulta, &datos_csv[pos_col]) {
            let valor = match dato_consulta {
                Dato::Interger(i) => i.to_string(),
                Dato::String(j) => j.to_string(),
            };

            Ok(valor)
        } else {
            return Err(errors::SqlError::Error);
        }
    } else {
        Ok(dato_consulta.to_string())
    }
}
