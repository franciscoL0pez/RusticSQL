use crate::errors;
use crate::errors::SqlError;
use crate::parseador_recursivo::parsear_condicion;
use crate::tipo_de_datos;
use std::collections::HashMap;
use std::fs::remove_file;
use std::fs::rename;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
///Funcion para obtener la posicion de un campo en el header
/// #Recibe la clave y el header
/// -Itera en el header y si encuentra la clave devuelve la posicion
/// -En caso de que no encuentre la clave devuelve un error
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
        Err(errors::SqlError::InvalidTable)
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
        ruta_de_csv
    } else {
        let ruta_de_csv = format!("{}{}", nombre_del_csv, ".csv");
        ruta_de_csv
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
    let archivo = match File::open(ruta_csv) {
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
    set_campos: Vec<String>,
    condiciones: Vec<String>,
) -> Result<(), SqlError> {
    let (archivo, ruta_archivo_temporal, _archivo_tem) = match abrir_y_crear_un_archivo(&ruta_csv) {
        Ok((archivo, ruta_archivo_temporal, _archivo_tem)) => {
            (archivo, ruta_archivo_temporal, _archivo_tem)
        }
        Err(e) => return Err(e),
    };
    let lector = BufReader::new(archivo);

    let condiciones: Vec<&str> = condiciones.iter().map(|s| s.as_str()).collect();
    let mut pos = 0;

    let condiciones_parseadas = match parsear_condicion(&condiciones, &mut pos, &header) {
        Ok(condiciones) => condiciones,
        Err(e) => {
            return Err(e);
        }
    };

    for (index, linea) in lector.lines().enumerate() {
        //Salteo la primera linea para no leer el header
        if index == 0 {
            continue;
        }
        let linea = match linea {
            Ok(linea) => linea,
            Err(_) => {
                return Err(errors::SqlError::Error);
            }
        };

        let fila: HashMap<String, String> = header
            .iter()
            .zip(linea.split(',').map(|s| s.trim().to_string()))
            .map(|(a, b)| (a.to_string(), b))
            .collect();

        let cumple_condiciones = match condiciones_parseadas.execute(&fila) {
            Ok(resultado) => resultado,

            Err(e) => return Err(e),
        };

        let fila: Vec<String> = linea.split(',').map(|s| s.trim().to_string()).collect();

        if cumple_condiciones {
            let nueva_linea = match cambiar_valores(fila, &set_campos, &header, &ruta_csv) {
                Ok(nueva_linea) => nueva_linea,

                Err(e) => {
                    let _ = remove_file(&ruta_archivo_temporal);
                    return Err(e);
                }
            };

            escribir_csv(&ruta_archivo_temporal, &nueva_linea)?;
        } else {
            escribir_csv(&ruta_archivo_temporal, &fila.join(",").to_string())?;
        }
    }

    let _ = rename(&ruta_archivo_temporal, ruta_csv);
    Ok(())
}

///Funcion para borrar las lineas del csv durante la consulta DELETE
///#Recibe por parametro el header, ruta del csv y las ccondiciones
/// -Creamos un archivo auxiliar, leeemos el archivo con los datos originales
/// -Itera en el csv y si encontramos que las lineas cumplen las condiciones pedidas en la consulta no escribimos en el archivo
/// -En caso contrario escribimos la linea en nuestro archivo axuliar
/// -Finalmente renombramos a nuestro archivo original como nuestro archivo aux
/// -En caso de que no se pueda abrir o crear un archivo devuelve un error
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

    let condiciones: Vec<&str> = condiciones.iter().map(|s| s.as_str()).collect();
    let mut pos = 0;
    
    let condiciones_parseadas = match parsear_condicion(&condiciones, &mut pos, &header) {
        Ok(condiciones) => condiciones,
        Err(e) => {
            return Err(e);
        }
    };

    for (index, linea) in lector.lines().enumerate() {
        //Salteo la primera linea para no leer el header
        if index == 0 {
            continue;
        }
        let linea = match linea {
            Ok(linea) => linea,
            Err(_) => {
                return Err(errors::SqlError::Error);
            }
        };

        let fila: HashMap<String, String> = header
            .iter()
            .zip(linea.split(',').map(|s| s.trim().to_string()))
            .map(|(a, b)| (a.to_string(), b))
            .collect();

        let cumple_condiciones = match condiciones_parseadas.execute(&fila) {
            Ok(resultado) => resultado,

            Err(e) => return Err(e),
        };

        let fila: Vec<String> = linea.split(',').map(|s| s.trim().to_string()).collect();

        //Si cumple las condicioens no escribo en el archivo ya que la quiero borrar
        if cumple_condiciones {
            print!("");
        } else {
            escribir_csv(&ruta_archivo_temporal, &fila.join(",").to_string())?
        }
    }

    let _ = rename(ruta_archivo_temporal, ruta_csv);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consultas::lock_test::{_acquire_lock, _release_lock};

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
}
