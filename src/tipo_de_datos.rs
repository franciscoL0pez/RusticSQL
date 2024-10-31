use crate::{
    errors::{self, SqlError},
    manejo_de_csv,
};

///Asumo para hacer la comprobacion de los datos siempre voy a tener un csv con un header y un primer registro
///Que no este mal ingresado
#[derive(Debug)]
pub enum Dato {
    String(String),
    Interger(i64),
}
///Convierto los valores de la 2 linea de mi csv a datos
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

pub fn convertir_a_dato(valor: &str) -> Dato {
    if let Ok(numero) = valor.parse::<i64>() {
        Dato::Interger(numero)
    } else {
        Dato::String(valor.to_string())
    }
}
//Me interesa si es true o false para lanzar el erorr
pub fn comparar_datos(dato_consulta: &Dato, dato_csv: &Dato) -> bool {
    match (dato_consulta, dato_csv) {
        (Dato::Interger(_), Dato::Interger(_)) => true,
        (Dato::String(_), Dato::String(_)) => true,
        _ => false,
    }
}
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
