use crate::manejo_de_csv::escribir_csv;
///Funcion que se encarga de manejar la consulta "INSERT"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos e insertar los datos
use crate::{errors::SqlError, manejo_de_csv, manejo_de_string};
pub fn insert(consulta_sql: &str, ruta_del_archivo: &str) -> Result<(), SqlError> {
    let (direccion_y_columnas, valores, columnas) =
        match manejo_de_string::separar_datos(consulta_sql) {
            Ok((direccion_y_columnas, valores, columnas)) => {
                (direccion_y_columnas, valores, columnas)
            }

            Err(e) => {
                return Err(e);
            }
        };

    let ruta = manejo_de_csv::obtener_ruta_del_csv(ruta_del_archivo, &direccion_y_columnas);
    let header = match manejo_de_csv::leer_header(&ruta, 0) {
        Ok(header) => header,

        Err(e) => {
            return Err(e);
        }
    };
    if columnas > header {
        return Err(SqlError::InvalidColumn);
    }

    let matriz = match manejo_de_string::crear_matriz(valores, columnas, &header, &ruta) {
        Ok(matriz) => matriz,

        Err(e) => {
            return Err(e);
        }
    };
    for fila in matriz.iter() {
        let linea = fila.join(",");

        if header.len() < fila.len() {
            return Err(SqlError::Error);
        }
        match escribir_csv(&ruta, &linea) {
            Ok(_) => (),
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        consultas::lock_test::{_acquire_lock, _release_lock},
        errors, realizar_consulta,
    };
    use std::{
        fs::{remove_file, File},
        io::{BufRead, BufReader, BufWriter, Write},
    };

    #[test]
    fn test_inserto_una_nueva_fila_a_un_csv() {
        _acquire_lock();
        let nombre_del_csv = "test.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        realizar_consulta(
            "INSERT INTO test (id,nombre,apellido) VALUES (2,juan,lopez)",
            " ",
        )
        .expect("No se pudo insertar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        lineas.next();
        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "2,juan,lopez");

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");

        _release_lock();
    }

    #[test]
    fn realizo_un_insert_con_varis_filas() {
        _acquire_lock();

        let nombre_del_csv = "test2.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        realizar_consulta(
            "INSERT INTO test2 (id,nombre,apellido) VALUES (2,juan,lopez),(3,pedro,lopez)",
            " ",
        )
        .expect("No se pudo insertar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        lineas.next();
        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "2,juan,lopez");
        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "3,pedro,lopez");

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");

        _release_lock();
    }

    #[test]
    fn realizo_un_insert_con_menos_columnas_que_las_que_tiene_el_header() {
        _acquire_lock();

        let nombre_del_csv = "test3.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        realizar_consulta("INSERT INTO test3 (id,nombre) VALUES (2,juan)", " ")
            .expect("No se pudo insertar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        lineas.next();
        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "2,juan,");

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");

        _release_lock();
    }
    //Si me tiran una columna que no esta en el header devuelvo un error de columna invalida
    #[test]
    fn relizo_un_insert_con_mas_columnas_que_las_que_tiene_el_header() {
        _acquire_lock();
        let nombre_del_csv = "test4.csv";
        let error2 = errors::SqlError::InvalidColumn;
        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        match realizar_consulta(
            "INSERT INTO test4 (id,nombre,apellido,s) VALUES (2,juan,lopez,20)",
            " ",
        ) {
            Ok(_) => (),
            Err(e) => {
                assert_eq!(e, error2);
            }
        }

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
        _release_lock();
    }

    #[test]
    fn realizo_un_insert_con_datos_incorrectos() {
        _acquire_lock();
        let nombre_del_csv = "test5.csv";
        let error = errors::SqlError::Error;
        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        match realizar_consulta(
            "INSERT INTO test5 (id,nombre,apellido) VALUES (2,fran,2)",
            " ",
        ) {
            Ok(_) => (),
            Err(e) => {
                assert_eq!(e, error);
            }
        }

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
        _release_lock();
    }
}
