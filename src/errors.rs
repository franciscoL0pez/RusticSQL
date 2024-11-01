use std::fmt::Display;
/// Enuma para representar los distintos tipos de errores que pueden ocurrir en la consulta SQL
///
/// Los posibles errores son:
///
/// - `InvalidTable`: Para problemas relacionados con las tablas
/// - `InvalidColumn`: Para problemas relacionados con las columnas
/// - `InvalidSyntax`: Para problemas de sintaxis en la consulta
/// - `Error`: Error generico para otros problemas.
///
#[derive(Debug, PartialEq)]
pub enum SqlError {
    InvalidTable,
    InvalidColumn,
    InvalidSyntax,
    Error,
}

impl Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::InvalidTable => write!(f, "[InvalidTable]: [Error to process table]"),
            SqlError::InvalidColumn => write!(f, "[InvalidColumn]: [Error to process column]"),
            SqlError::InvalidSyntax => write!(f, "[InvalidSyntax]: [Error to process query]"),
            SqlError::Error => write!(f, "[Error]: [An error occurred]"),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{
        fs::{remove_file, File},
        io::{BufRead, BufReader},
    };

    use crate::manejo_de_csv::escribir_csv;
    use crate::{manejo_de_csv::obtener_posicion_header, realizar_consulta};

    use super::*;

    #[test]
    fn intento_obtener_la_posicion_de_columnas_invalidas() {
        let error = SqlError::InvalidColumn;
        let header = vec![
            "id".to_string(),
            "nombre".to_string(),
            "apellido".to_string(),
        ];
        let clave = "producto".to_string();

        match obtener_posicion_header(&clave, &header) {
            Ok(_) => panic!("No deberia haber pasado"),
            Err(e) => assert_eq!(e, error),
        }
    }

    #[test]
    fn intento_procesar_una_query_que_no_existe() {
        let error = SqlError::InvalidSyntax;
        let query = "UPSERT";

        match realizar_consulta(query, "test.csv") {
            Ok(_) => panic!("No deberia haber pasado"),
            Err(e) => assert_eq!(e, error),
        }
    }

    #[test]
    fn query_para_una_tabla_que_no_existe() {
        let error = SqlError::InvalidTable;
        let ruta_csv = "archivos/tabla";
        let linea = "producto 110";

        match escribir_csv(&ruta_csv, linea) {
            Ok(_) => panic!("No deberia haber pasado"),
            Err(e) => assert_eq!(e, error), // Separados por coma, sin paréntesis extras
        }
    }

    #[test]
    fn intento_realizar_una_query_con_errores_de_syntaxis() {
        let error = SqlError::InvalidSyntax;
        let consulta = "INSERTS INTOD tabla (id, nombre, apellido) VALUES (1, 'Juan', 'Perez')";

        match realizar_consulta(consulta, "test.csv") {
            Ok(_) => panic!("No deberia haber pasado"),
            Err(e) => assert_eq!(e, error),
        }
    }

    #[test]
    fn intento_leer_un_csv_vacio() {
        let error = SqlError::Error;
        let ruta_csv = "test_invalid_1.csv".to_string();
        let _ = File::create(&ruta_csv).unwrap();
        let archivo = File::open(&ruta_csv).unwrap();

        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();

        match lineas.next() {
            Some(_) => panic!("No deberia haber pasado"),
            None => assert_eq!(SqlError::Error, error),
        }
        remove_file(&ruta_csv).unwrap();
    }
}
