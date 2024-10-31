use crate::condiciones;
use crate::errors::SqlError;
use crate::manejo_de_csv;
use crate::manejo_de_csv::escribir_csv;
use crate::manejo_de_string;


///Funcion que se encarga de manejar la consulta "INSERT"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos e insertar los datos
pub fn insert(consulta_sql: &str, ruta_del_archivo: &str) -> Result<(),SqlError> {
 
    let (direccion_y_columnas, valores, columnas) =
        match manejo_de_string::separar_datos(&consulta_sql) {
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
        match escribir_csv(&ruta, &linea){
            Ok(_) => (),
            Err(e) => {
                return Err(e);
            }
        }
        };
    Ok(())

}


///Funcion que se encarga de manejar la consulta "UPDATE"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos y realizar el update
pub fn update(consulta_sql: &str, ruta_del_archivo: &str) -> Result<(),SqlError> {
    let (nombre_del_csv, campos_para_actualizar, donde_actualizar) =
        match manejo_de_string::separar_datos_update(&consulta_sql) {
            Ok((nombre_del_csv, campos_para_actualizar, donde_actualizar)) => {
                (nombre_del_csv, campos_para_actualizar, donde_actualizar)
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

    let _ = manejo_de_csv::actualizar_csv(ruta_csv, header, campos_para_actualizar, donde_actualizar);
    Ok(())
}

///Funcion que se encarga de manejar la consulta "UPDATE"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos y realizar el delete
pub fn delete(consulta_sql: &str, ruta_del_archivo: &str) -> Result<(), SqlError> {
    let (nombre_del_csv, condiciones) = match manejo_de_string::separar_datos_delete(consulta_sql) {
        Ok((nombre_del_csv, condiciones)) => (nombre_del_csv, condiciones),

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

    let _ = manejo_de_csv::borrar_lineas_csv(ruta_csv, header, condiciones);
    Ok(())
}

///Funcion para ordenar las lineas cuando se hace un SELECT
/// Recibe la matriz, el header y un vector ordenamiento, con la condicion y si es ASC o DESC
fn ordenar_matriz(
    matriz: Vec<Vec<String>>,
    ordenamiento: Vec<String>,
    header: &[String],
) -> Result<Vec<Vec<String>>, String> {
    let mut matriz = matriz;
    let fila_1 = matriz.remove(0);

    let pos = match manejo_de_csv::obtener_posicion_header(&ordenamiento[1].to_lowercase(), header)
    {
        Ok(pos) => pos,

        Err(e) => {
            return Err(e.to_string());
        }
    };

    if ordenamiento[2] == "ASC" {
        matriz.sort_by(|a, b| a[pos].cmp(&b[pos]));
    } else if ordenamiento[2] == "DESC" {
        matriz.sort_by(|a, b| b[pos].cmp(&a[pos]));
    }

    matriz.insert(0, fila_1);

    Ok(matriz)
}
///Funcion para mostrar las columnas que se piden durante el SELECT
///Segun las columnas seleccionadas en el SELECT recibe la matriz previamente armada y muestra en orden dichas columnas con sus datos.
fn mostrar_select(
    matriz: Vec<Vec<String>>,
    columnas_selec: String,
    header: &[String],
    ordenamiento: Vec<String>,
) {
    let columnas_selec: Vec<String> = columnas_selec
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let mut posiciones: Vec<usize> = Vec::new();

    for valor in &columnas_selec {
        match manejo_de_csv::obtener_posicion_header(valor, header) {
            Ok(pos) => posiciones.push(pos),

            Err(e) => {
                println!("{}", e);
                return;
            }
        };
    }

    let matriz = match ordenar_matriz(matriz, ordenamiento, header) {
        Ok(matriz) => matriz,

        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    for fila in &matriz {
        let fila_ord: Vec<String> = posiciones.iter().map(|&i| fila[i].to_string()).collect();

        println!("{}", fila_ord.join(","));
    }
}

///Funcion que se encarga de manejar la consulta "SELECT"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos y realizar el SELECT
pub fn select(consulta_sql: &str, ruta_del_archivo: &str) -> Result<(),SqlError> {
    let (nombre_csv, mut columnas, condiciones) =
        match manejo_de_string::separar_datos_select(&consulta_sql) {
            Ok((nombre_csv, columnas, condiciones)) => (nombre_csv, columnas, condiciones),

            Err(e) => {
                return Err(e);
            }
        };

    let (condiciones, ordenamiento) = manejo_de_string::separar_order(condiciones);

    let condiciones_parseadas = match condiciones::procesar_condiciones(condiciones) {
        Ok(condiciones) => condiciones,
        Err(e) => {
            return Err(e);
        }
    };
    let ruta_csv = manejo_de_csv::obtener_ruta_del_csv(ruta_del_archivo, &nombre_csv);

    let header = match manejo_de_csv::leer_header(&ruta_csv, 0) {
        Ok(header) => header,

        Err(e) => {
            return Err(e);
        }
    };

    let matriz = match condiciones::comparar_con_csv(condiciones_parseadas, ruta_csv, &header) {
        Ok(matriz) => matriz,

        Err(e) => {
            return Err(e);
        }
    };

    if columnas == "*" {
        columnas = header.join(",");
    }

    mostrar_select(matriz, columnas, &header, ordenamiento);
    Ok(())
}

#[cfg(test)] 
mod tests {
    use std::{fs::{remove_file, File}, io::{BufRead, BufReader, BufWriter, Write}};

    use crate::{errors, realizar_consulta};

    #[test]
    fn test_inserto_una_nueva_fila_a_un_csv() {
        let nombre_del_csv = "test.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("INSERT INTO test (id,nombre,apellido) VALUES (2,juan,lopez)", " ").expect("No se pudo insertar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        lineas.next();
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "2,juan,lopez");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
        

    }

    #[test]
    fn realizo_un_insert_con_varis_filas(){
        let nombre_del_csv = "test2.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("INSERT INTO test2 (id,nombre,apellido) VALUES (2,juan,lopez),(3,pedro,lopez)", " ").expect("No se pudo insertar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        lineas.next();
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "2,juan,lopez");
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "3,pedro,lopez");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }

    #[test]
    fn realizo_un_insert_con_menos_columnas_que_las_que_tiene_el_header(){
        let nombre_del_csv = "test3.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("INSERT INTO test3 (id,nombre) VALUES (2,juan)", " ").expect("No se pudo insertar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        lineas.next();
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "2,juan,");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }
    //Si me tiran una columna que no esta en el header devuelvo un error de columna invalida
    #[test]
    fn relizo_un_insert_con_mas_columnas_que_las_que_tiene_el_header(){
        let nombre_del_csv = "test4.csv";
        let error2 = errors::SqlError::InvalidColumn;
        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        match realizar_consulta("INSERT INTO test4 (id,nombre,apellido,s) VALUES (2,juan,lopez,20)", " ") {
            Ok(_) => (),
            Err(e) => {
                assert_eq!(e, error2);
            }
        }

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }

    #[test]
    fn realizo_un_insert_con_datos_incorrectos(){
        let nombre_del_csv = "test5.csv";
        let error = errors::SqlError::Error;
        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        match realizar_consulta("INSERT INTO test5 (id,nombre,apellido) VALUES (2,fran,2)", " ") {
            Ok(_) => (),
            Err(e) => {
                assert_eq!(e, error);
            }
        }

        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }

    #[test]
    fn realizo_un_update_con_una_condicion(){
        let nombre_del_csv = "test6.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("UPDATE test6 SET nombre = fran WHERE id = 2", " ").expect("No se pudo actualizar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "1,carlos,lopez");
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "2,fran,lopez");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }

    #[test]
    fn realizo_un_update_con_and_y_or(){
        let nombre_del_csv = "test7.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,perez").expect("No se pudo escribir la fila");
        writeln!(writer, "3,roberto,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("UPDATE test7 SET nombre = fran WHERE id = 2 or nombre = carlos", " ").expect("No se pudo actualizar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "1,fran,lopez");
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "2,fran,perez");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }

    #[test]
    fn realizo_un_update_con_and_not_y_or(){
        let nombre_del_csv = "test8.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "3,roberto,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("UPDATE test8 SET nombre = fran WHERE id = 2 or nombre = carlos and apellido = lopez not id = 1", " ").expect("No se pudo actualizar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "1,carlos,lopez");
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "2,fran,lopez");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }

    #[test]
    fn realizo_un_delete_con_una_condicion(){
        let nombre_del_csv = "test9.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "3,roberto,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("DELETE FROM test9 WHERE id = 2", " ").expect("No se pudo borrar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();

        //Si se borro la linea deben cambiar de lugar 
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "1,carlos,lopez");
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "3,roberto,lopez");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }

    #[test]
    fn realizo_un_delete_con_and_or_not(){
        let nombre_del_csv = "test10.csv";

        let archivo = File::create(nombre_del_csv).expect("No se pudo crear el archivo");
        let mut writer = BufWriter::new(archivo);
    
        writeln!(writer, "id,nombre,apellido").expect("No se pudo escribir el header");
        writeln!(writer, "1,carlos,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "2,juan,lopez").expect("No se pudo escribir la fila");
        writeln!(writer, "3,roberto,lopez").expect("No se pudo escribir la fila");
    
        writer.flush().expect("No se pudo cerrar el archivo correctamente");
    
        realizar_consulta("DELETE FROM test10 WHERE id = 2 or nombre = carlos and apellido = lopez not id = 1", " ").expect("No se pudo borrar la fila");

        let archivo = File::open(nombre_del_csv).expect("No se pudo abrir el archivo");
        let lector = BufReader::new(archivo);
        let mut lineas = lector.lines();
        lineas.next();

        //Si se borro la linea deben cambiar de lugar 
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "1,carlos,lopez");
        let linea = lineas.next().expect("No se pudo leer la linea").expect("No se pudo leer la linea");
        assert_eq!(linea, "3,roberto,lopez");

        
        remove_file(nombre_del_csv).expect("No se pudo eliminar el archivo");
    }


}
