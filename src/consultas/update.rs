use crate::{errors::SqlError, manejo_de_csv, manejo_de_string};

///Funcion que se encarga de manejar la consulta "UPDATE"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos y realizar el update
pub fn update(consulta_sql: &str, ruta_del_archivo: &str) -> Result<(), SqlError> {
    let (nombre_del_csv, campos_para_actualizar, condiciones) =
        match manejo_de_string::separar_datos_update(consulta_sql) {
            Ok((nombre_del_csv, campos_para_actualizar, condiciones)) => {
                (nombre_del_csv, campos_para_actualizar, condiciones)
            }

            Err(e) => {
                return Err(e);
            }
        };

    let ruta_csv = manejo_de_csv::obtener_ruta_del_csv(ruta_del_archivo, &nombre_del_csv);

    let header = match manejo_de_csv::leer_header(&ruta_csv, 0) {
        Ok(header) => header,

        Err(e) => {
            return Err(e);
        }
    };

    let _ = manejo_de_csv::actualizar_csv(ruta_csv, header, campos_para_actualizar, condiciones);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{remove_file, File},
        io::{BufRead, BufReader, BufWriter, Write},
    };

    use crate::{
        consultas::lock_test::{_acquire_lock, _release_lock},
        realizar_consulta,
    };

    #[test]
    fn realizo_un_update_con_una_condicion() {
        _acquire_lock();
        let nombre_del_csv = "test6.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        realizar_consulta("UPDATE test6 SET nombre = fran WHERE id = 2", " ")
            .expect("No se pudo actualizar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();

        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "1,carlos,lopez");
        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "2,fran,lopez");

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
        _release_lock();
    }

    #[test]
    fn realizo_un_update_con_and_y_or() {
        _acquire_lock();
        let nombre_del_csv = "test7.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,perez").expect("No se pudo escribir la fila");
        writeln!(writer, "3,roberto,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        realizar_consulta(
            "UPDATE test7 SET nombre = fran WHERE id = 2 OR (nombre = carlos AND apellido = lopez);",
            " ",
        )
        .expect("No se pudo actualizar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();

        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "1,fran,lopez");
        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "2,fran,perez");

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
        _release_lock();
    }

    #[test]
    fn realizo_un_update_con_and_not_y_or() {
        _acquire_lock();
        let nombre_del_csv = "test8.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);

        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,mercedes").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "3,roberto,lopez").expect("No se pudo escribir la fila");

        writer
            .flush()
            .expect("No se pudo cerrar el archivo correctamente");

        realizar_consulta(
            "UPDATE test8 SET nombre = fran WHERE NOT (id = 99 OR apellido = lopez);",
            " ",
        )
        .expect("No se pudo actualizar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();

        let linea = lineas
            .next()
            .expect("No se pudo leer la linea")
            .expect("No se pudo leer la linea");
        assert_eq!(linea, "1,fran,mercedes");

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
        _release_lock();
    }
}
