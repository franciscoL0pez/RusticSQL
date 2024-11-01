use crate::condiciones;
use crate::errors;
use crate::errors::SqlError;
use crate::tipo_de_datos;
use std::fs::remove_file;
use std::fs::rename;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

pub fn obtener_posicion_header(clave: &str, header: &[String]) -> Result<usize, SqlError> {
    let pos = header.iter().position(|s| s == clave);

    match pos {
        Some(i) => Ok(i),
        None => Err(errors::SqlError::InvalidColumn),
    }
}


///Leo el header de un csv 
///# Recibe la ruta del archivo y la cantidad de lineas a ignorar
///- Devuelve un vector con el header
///- En caso de que no se pueda leer el archivo devuelve un error
pub fn leer_header(archivo: &String, linenas_a_ignorar: i64) -> Result<Vec<String>, SqlError> {
    let path = Path::new(archivo);
    let file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return Err(errors::SqlError::InvalidTable),
    };

    let reader = io::BufReader::new(file);

    let mut lineas = reader.lines();

    //Leo la primera ya que quiero saber como es la estructura de mi archivo
    //Devuelvo el header o en caso de que no es
    for _ in 0..linenas_a_ignorar {
        lineas.next();
    }

    if let Some(header_line) = lineas.next() {
        let header_line = match header_line {
            Ok(header_line) => header_line,
            Err(_) => return Err(SqlError::InvalidTable),
        };

        let header: Vec<String> = header_line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        Ok(header)
    } else {
        return Err(errors::SqlError::InvalidTable);
    }
}
///Funcion par obtener la ruta donde se encuentran nuestros csv
///#Le pasamos la direccion de la ruta (la que pasamos en la consulta) y el nombre del csv
/// -Luego une los strings y devuelve la ruta
/// -En caso de que la ruta sea menor a 3 caracteres devuelve solo el nombre del csv
/// -En caso contrario devuelve la ruta completa
pub fn obtener_ruta_del_csv(ruta: &str, nombre_del_csv: &str) -> String {
    let palabras: Vec<&str> = nombre_del_csv.split(" ").collect();
    let nombre_del_csv = palabras[0];

    if ruta.len() > 3 {
        let ruta_de_csv = format!("{}{}{}{}", ruta, "/", nombre_del_csv, ".csv");
        return ruta_de_csv;
    } else {
        let ruta_de_csv = format!("{}{}", nombre_del_csv, ".csv");
        return ruta_de_csv;
    }
}
///Funcion para escribir una linea en un csv
///Abre el archivo y escribe una linea en el
/// #Recibe la ruta del csv y la linea a escribir
/// -En caso de que no exista el archivo devuelve un error
/// -En caso contrario escribe la linea en el archivo
pub fn escribir_csv(ruta_csv: &str, linea: &str) -> Result<(), SqlError> {
    if !Path::new(&ruta_csv).exists() {
        return Err(errors::SqlError::InvalidTable);
    }

    let mut archivo = match OpenOptions::new().append(true).open(ruta_csv) {
        Ok(archivo) => archivo,
        Err(_) => return Err(errors::SqlError::InvalidTable),
    };

    let _ = writeln!(archivo, "{}", linea);

    Ok(())
}
///Funcion para cambiar los valores de una linea en el csv
/// #Recibe una linea, los campos a cambiar, el header y la ruta del csv
/// -Obtiene la posicion del campo a cambiar en el header
/// -Comprueba el tipo de dato que se quiere cambiar
/// -Cambia el valor en la linea
/// -Devuelve la linea con los valores cambiados
/// -En caso de que no se pueda cambiar el valor devuelve un error
pub fn cambiar_valores(
    linea: Vec<String>,
    campos_a_cambiar: &[String],
    header: &[String],
    ruta_csv: &String,
) -> Result<String, SqlError> {
    let mut linea = linea;

    let pos = match obtener_posicion_header(&campos_a_cambiar[0], header) {
        Ok(pos) => pos,

        Err(e) => {
            return Err(e);
        }
    };

    let valor = match tipo_de_datos::comprobar_dato(&campos_a_cambiar[2], ruta_csv, pos) {
        Ok(valor) => valor,

        Err(e) => return Err(e),
    };

    linea[pos] = valor;

    let nueva_linea = linea.join(",");

    Ok(nueva_linea)
}


/// Funcion para abrir y crear un archivo
/// #Recibe la ruta del csv
/// -Abre el archivo y en caso de que no exista devuelve un error
/// -Crea un archivo auxiliar
/// -Devuelve los archivos abiertos
/// -En caso de que no se pueda abrir o crear un archivo devuelve un error
/// -En caso contrario devuelve los archivos abiertos
/// -En caso de que no se pueda abrir o crear un archivo devuelve un error
fn abrir_y_crear_un_archivo(ruta_csv: &str) -> Result<(File, String, File), SqlError> {
    let archivo = match File::open(&ruta_csv) {
        Ok(archivo) => archivo,
        Err(_) => {
            return Err(errors::SqlError::Error);
        }
    };

    let ruta_archivo_temporal = "auxiliar.csv".to_string();

    let _archivo_tem = match File::create(&ruta_archivo_temporal) {
        Ok(archivo) => archivo,
        Err(_) => {
            return Err(errors::SqlError::Error);
        }
    };

    Ok((archivo, ruta_archivo_temporal, _archivo_tem))
}



/* 
///Funcion para actualizar las lineas del csv durante la consulta UPDATE
///#Recibe por parametro el header, ruta del csv, la clave y los campos a actualizar
///-Creamos un archivo auxiliar, leeemos el archivo con los datos originales y obtenemos la posicion donde se encuentra nuestra clave comparandla con el header
///-En caso de que esta no se encuentre lanza un error
///-Itera en el csv y si encontramos que coinciden cambiamos los valores pedidos en la consulta
///-Escribimos la linea actualiza en nuestro archivo axuliar
///-Finalmente renombramos a nuestro archivo original como nuestro archivo aux
pub fn actualizar_csv(
    ruta_csv: String,
    header: Vec<String>,
    campos_a_cambiar: Vec<String>,
    claves: Vec<String>,
) -> Result<(), SqlError> {
    let (archivo, ruta_archivo_temporal, _archivo_tem) = match abrir_y_crear_un_archivo(&ruta_csv) {
        Ok((archivo, ruta_archivo_temporal, _archivo_tem)) => {
            (archivo, ruta_archivo_temporal, _archivo_tem)
        }
        Err(e) => return Err(e),
    };
    let lector = BufReader::new(archivo);

    let condiciones_parseadas = match condiciones::procesar_condiciones(claves) {
        Ok(condiciones) => condiciones,

        Err(_) => {
            return Err(errors::SqlError::InvalidSyntax);
        }
    };

    for linea in lector.lines() {
        let linea_csv: Vec<String> = match linea {
            Ok(linea) => linea,
            Err(_) => {
                let _ = remove_file(&ruta_archivo_temporal);
                return Err(errors::SqlError::Error);
            }
        }
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

        let cumple_condiciones =
            match condiciones::comparar_op_logico(&condiciones_parseadas, &linea_csv, &header) {
                Ok(segundo_resultado) => segundo_resultado,

                Err(_) => {
                    let _ = remove_file(&ruta_archivo_temporal);
                    return Err(errors::SqlError::InvalidSyntax);
                }
            };

        if cumple_condiciones && &linea_csv.join(",") != &header.join(",") {
            let nueva_linea =
                match cambiar_valores(linea_csv, &campos_a_cambiar, &header, &ruta_csv) {
                    Ok(nueva_linea) => nueva_linea,

                    Err(e) => {
                        let _ = remove_file(&ruta_archivo_temporal);
                        return Err(e);
                    }
                };

            escribir_csv(&ruta_archivo_temporal, &nueva_linea)?;
        } else {
            escribir_csv(&ruta_archivo_temporal, &linea_csv.join(",").to_string())?;
        }
    }

    let _ = rename(&ruta_archivo_temporal, ruta_csv);
    Ok(())
}

///Funcion para borrar las lineas del csv durante la consulta DELETE
///#Recibe por parametro el header, ruta del csv y la clave
///-Creamos un archivo auxiliar, leeemos el archivo con los datos originales y obtiene la posicion donde se encuentra nuestra clave comparandla con el header
///-En caso de que esta no se encuentre lanza un error
///-Itera en las lineas del csv y si encontramos que coinciden no los copiamos en nuestro archivo aux


pub fn borrar_lineas_csv(
    ruta_csv: String,
    header: Vec<String>,
    condiciones: Vec<String>,
) -> Result<(), SqlError> {
    let (archivo, ruta_archivo_temporal, _archivo_tem) = match abrir_y_crear_un_archivo(&ruta_csv) {
        Ok((archivo, ruta_archivo_temporal, _archivo_tem)) => {
            (archivo, ruta_archivo_temporal, _archivo_tem)
        }
        Err(e) => return Err(e),
    };

    let lector = BufReader::new(&archivo);

    let condiciones_parseadas = match condiciones::procesar_condiciones(condiciones) {
        Ok(condiciones) => condiciones,

        Err(e) => {
            return Err(e);
        }
    };

    for linea in lector.lines() {
        let linea_csv: Vec<String> = match linea {
            Ok(linea) => linea,
            Err(_) => {
                let _ = remove_file(&ruta_archivo_temporal);
                return Err(errors::SqlError::Error);
            }
        }
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

        let cumple_condiciones =
            match condiciones::comparar_op_logico(&condiciones_parseadas, &linea_csv, &header) {
                Ok(segundo_resultado) => segundo_resultado,

                Err(e) => {
                    let _ = remove_file(&ruta_archivo_temporal);
                    return Err(e);
                }
            };
        //Si cumple las condicioens no escribo en el archivo ya que la quiero borrar
        if cumple_condiciones && &linea_csv.join(",") != &header.join(",") {
        } else {
            escribir_csv(&ruta_archivo_temporal, &linea_csv.join(",").to_string())?
        }
    }

    let _ = rename(ruta_archivo_temporal, ruta_csv);
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::consultas::lock_test::{_acquire_lock, _release_lock};
    use std::fs::remove_file;

    #[test]
    fn test01leer_header_y_devolverlo() {
        _acquire_lock();
        let direccion_del_archivo = "Archivos_Csv/ordenes.csv".to_string();

        let resultado = leer_header(&direccion_del_archivo, 0);
        assert!(resultado.is_ok());

        let header = resultado.unwrap();
        assert_eq!(header, vec!["id", "id_cliente", "producto", "cantidad"]);
        _release_lock();
    }

    #[test]
    fn test02leemos_el_header_y_ocurre_un_error() {
        let direccion_del_archivo = "".to_string();

        let resultado = leer_header(&direccion_del_archivo, 0);
        assert!(resultado.is_err());
    }

    #[test]
    fn test03se_actualiza_el_csv_segun_una_clave() {
        _acquire_lock();
        //Para testear esta funcion creo un archivo de prueba y lo elimino al final
        let ruta_csv = "test_manejo.csv".to_string();
        let mut archivo = File::create(&ruta_csv).unwrap();

        let header = vec!["id".to_string(), "nombre".to_string(), "edad".to_string()];
        let campos = vec!["edad".to_string(), "=".to_string(), "40".to_string()];
        let clave = vec!["id".to_string(), "=".to_string(), "1".to_string()];

        //Le pongo algunos datos para el test
        let datos_in = "id,nombre,edad\n1,Juan,25\n2,Maria,30\n";
        archivo.write_all(datos_in.as_bytes()).unwrap();
        drop(archivo);

        //Abuso un poquito del echo de que estamos probando un test y uso un clone para pasar ruta_csv
        actualizar_csv(ruta_csv.clone(), header, campos, clave).unwrap();

        let archivo = File::open(&ruta_csv).unwrap();
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();

        //Me quedo con la 2 linea ya que luego del header es la que actualice
        let _ = lineas.next().unwrap();
        let linea_actualizada = lineas.next().unwrap().unwrap();
        let linea_esperada = "1,Juan,40".to_string();

        remove_file(&ruta_csv).unwrap();
        assert_eq!(linea_esperada, linea_actualizada);
        _release_lock();
    }
}
*/