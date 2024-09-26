use crate::condiciones;
use crate::tipo_de_datos;
use std::fs::remove_file;
use std::fs::rename;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
//Por ahora leo el archivo, saco el header y atajo el error asi
pub fn leer_header(archivo: &String, linenas_a_ignorar: i64) -> io::Result<Vec<String>> {
    let path = Path::new(archivo);
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);

    let mut lineas = reader.lines();

    //Leo la primera ya que quiero saber como es la estructura de mi archivo
    //Devuelvo el header o en caso de que no es
    for _ in 0..linenas_a_ignorar {
        lineas.next();
    }

    if let Some(header_line) = lineas.next() {
        let header_line = header_line?;
        let header: Vec<String> = header_line
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        Ok(header)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "CSV_Error:Error al leer el csv",
        ))
    }
}
///Funcion par obtener la ruta donde se encuentran nuestros csv
///#Le pasamos la direccion de la ruta (la que pasamos en la consulta) y el nombre del csv
///Luego une los strings y devuelve la ruta
pub fn obtener_ruta_del_csv(ruta: String, nombre_del_csv: &str) -> String {
    let palabras: Vec<&str> = nombre_del_csv.split(" ").collect();
    let nombre_del_csv = palabras[0];

    let ruta_de_csv = format!("{}{}{}{}", ruta, "/", nombre_del_csv, ".csv");

    ruta_de_csv.to_string()
}
///Funcion para escribir una linea en un csv
///Abre el archivo y escribe una linea en el
pub fn escribir_csv(ruta_csv: &str, linea: &str) -> io::Result<()> {
    if !Path::new(&ruta_csv).exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("CSV_ERROR:El archivo CSV {} no existe", ruta_csv),
        ));
    }

    let mut archivo = OpenOptions::new().append(true).open(ruta_csv)?;

    writeln!(archivo, "{}", linea)?;

    Ok(())
}

pub fn cambiar_valores(
    linea: Vec<String>,
    campos_a_cambiar: &[String],
    header: &[String],
    ruta_csv: &String,
) -> Result<String, String> {
    let mut linea = linea;

    let pos = match obtener_posicion_header(&campos_a_cambiar[0], header) {
        Ok(pos) => pos,

        Err(e) => {
            return Err(e.to_string());
        }
    };


    let valor = match tipo_de_datos::comprobar_dato(&campos_a_cambiar[2], ruta_csv, pos) {
        Ok(valor) => {valor},
        

        Err(e) => {return Err(e.to_string())},
    };

    linea[pos] = valor;

    let nueva_linea = linea.join(",");

    Ok(nueva_linea)
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
    campos_a_cambiar: Vec<String>,
    claves: Vec<String>,
) -> io::Result<()> {
    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let ruta_archivo_temporal = "auxiliar.csv".to_string();
    let _ = File::create(&ruta_archivo_temporal)?;

    let condiciones_parseadas = condiciones::procesar_condiciones(claves);

    for linea in lector.lines() {
        let linea_csv: Vec<String> = linea?.split(',').map(|s| s.trim().to_string()).collect();

        let cumple_condiciones =
            match condiciones::comparar_op_logico(&condiciones_parseadas, &linea_csv, &header) {
                Ok(segundo_resultado) => segundo_resultado,

                Err(e) => {
                    let _ = remove_file(&ruta_archivo_temporal);
                    return Err(io::Error::new(io::ErrorKind::Other, format!("{}", e)));
                }
            };

            if cumple_condiciones && &linea_csv.join(",") != &header.join(",") {
                let nueva_linea = cambiar_valores(linea_csv, &campos_a_cambiar, &header, &ruta_csv)
                    .map_err(|e|{
                        println!("{}",e);
                        let _ = remove_file(&ruta_archivo_temporal);
                        io::Error::new(io::ErrorKind::Other, format!("{}", e))
                        })?;
    
                escribir_csv(&ruta_archivo_temporal, &nueva_linea)?
            } else {
                escribir_csv(&ruta_archivo_temporal, &linea_csv.join(",").to_string())?
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
///-Finalmente renombramos nuestro archivo auxiliar como si fuera el original
pub fn borrar_lineas_csv(
    ruta_csv: String,
    header: Vec<String>,
    condiciones: Vec<String>,
) -> io::Result<()> {
    let archivo = File::open(&ruta_csv)?;
    let lector = BufReader::new(archivo);
    let ruta_archivo_temporal = "auxiliar.csv";
    let _archivo_tem = File::create(ruta_archivo_temporal)?;
    
    let condiciones_parseadas = condiciones::procesar_condiciones(condiciones);
    print!("{:?}",condiciones_parseadas);
    for linea in lector.lines() {
        let linea_csv: Vec<String> = linea?.split(',').map(|s| s.trim().to_string()).collect();

        let cumple_condiciones = match condiciones::comparar_op_logico(&condiciones_parseadas, &linea_csv, &header) {
            Ok(segundo_resultado) => segundo_resultado,

            Err(e) => {
                let _ = remove_file(&ruta_archivo_temporal);
                return Err(io::Error::new(io::ErrorKind::Other, format!("{}", e)));
            }
        };
        //Si cumple las condicioens no escribo en el archivo ya que la quiero borrar
        if cumple_condiciones  && &linea_csv.join(",") != &header.join(","){
    
            } else {
                escribir_csv(&ruta_archivo_temporal, &linea_csv.join(",").to_string())?
            }
        }

        let _ = rename(ruta_archivo_temporal, ruta_csv);
    Ok(())
}


pub fn obtener_posicion_header(clave: &str, header: &[String]) -> Result<usize, String> {
    let pos = header.iter().position(|s| s == clave);

    match pos {
        Some(i) => Ok(i),
        None => Err("INVALID_COLUMN: La columna ingresada no se encuntra en el csv".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use super::*;

    #[test]
    fn test01leer_header_y_devolverlo() {
        let direccion_del_archivo = "Archivos_Csv/ordenes.csv".to_string();

        let resultado = leer_header(&direccion_del_archivo, 0);
        assert!(resultado.is_ok());

        let header = resultado.unwrap();
        assert_eq!(header, vec!["id", "id_cliente", "producto", "cantidad"]);
    }

    #[test]
    fn test02leemos_el_header_y_ocurre_un_error() {
        let direccion_del_archivo = "".to_string();

        let resultado = leer_header(&direccion_del_archivo, 0);
        assert!(resultado.is_err());
    }

    #[test]
    fn test03se_actualiza_el_csv_segun_una_clave() {
        //Para testear esta funcion creo un archivo de prueba y lo elimino al final
        let ruta_csv = "test.csv".to_string();
        let mut archivo = File::create(&ruta_csv).unwrap();

        let header = vec!["id".to_string(), "nombre".to_string(), "edad".to_string()];
        let campos = vec!["edad".to_string(),"=".to_string(), "40".to_string()];
        let clave = vec!["id".to_string(),"=".to_string() ,"1".to_string()];

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
    }

    #[test]
    fn test04se_elimina_del_csv_una_linea() {
        //Vuelvo a crear mi archivo de prueba
        let ruta_csv = "test_1.csv".to_string();
        let mut archivo = File::create(&ruta_csv).unwrap();

        let header = vec!["id".to_string(), "nombre".to_string(), "edad".to_string()];
        let clave = vec!["id".to_string(),"=".to_string() ,"1".to_string()];

        //Le pongo algunos datos para el test
        let datos_in = "id,nombre,edad\n1,Juan,25\n2,Maria,30\n";
        archivo.write_all(datos_in.as_bytes()).unwrap();
        drop(archivo);

        borrar_lineas_csv(ruta_csv.clone(), header, clave).unwrap();

        let archivo = File::open(&ruta_csv).unwrap();
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();

        //Me quedo con la 2 linea ya que luego del header es la que actualice
        let _ = lineas.next().unwrap();
        let linea_actualizada = lineas.next().unwrap().unwrap();
        let linea_esperada = "1,Juan,25".to_string();

        //Como elimine la 2 linea comparo y tienen que ser distintas
        remove_file(&ruta_csv).unwrap();
        assert_ne!(linea_esperada, linea_actualizada);
    }
}
