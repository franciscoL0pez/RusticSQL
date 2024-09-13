use crate::condiciones;
use crate::manejo_de_csv;
use crate::manejo_de_string;

///Funcion que se encarga de manejar la consulta "INSERT"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos e insertar los datos
pub fn insert(consulta_sql: String, ruta_del_archivo: String) {
    let (direccion_y_columnas, valores, columnas) =
        match manejo_de_string::separar_datos(consulta_sql) {
            Ok((direccion_y_columnas, valores, columnas)) => {
                (direccion_y_columnas, valores, columnas)
            }

            Err(e) => {
                println!("{}", e);
                return;
            }
        };

    let ruta = manejo_de_csv::obtener_ruta_del_csv(ruta_del_archivo, &direccion_y_columnas);
    let header = match manejo_de_csv::leer_header(&ruta) {
        Ok(header) => header,

        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let matriz = match manejo_de_string::crear_matriz(valores, columnas, &header) {
        Ok(matriz) => matriz,

        Err(e) => {
            println!("{}", e);
            return;
        }
    };
    for fila in matriz.iter() {
        let linea = fila.join(",");

        match manejo_de_csv::escribir_csv(ruta.to_string(), &linea) {
            Ok(_) => print!(""),
            Err(e) => {
                println!("{}", e);
                break;
            }
        };
    }
    
}

///Funcion que se encarga de manejar la consulta "UPDATE"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos y realizar el update
pub fn update(consulta_sql: String, ruta_del_archivo: String) {
    let (nombre_del_csv, campos_para_actualizar, donde_actualizar) =
        match manejo_de_string::separar_datos_update(consulta_sql) {
            Ok((nombre_del_csv, campos_para_actualizar, donde_actualizar)) => {
                (nombre_del_csv, campos_para_actualizar, donde_actualizar)
            }

            Err(e) => {
                println!("{}", e);
                return;
            }
        };

    let ruta_csv = manejo_de_csv::obtener_ruta_del_csv(ruta_del_archivo, &nombre_del_csv);

    let header = match manejo_de_csv::leer_header(&ruta_csv) {
        Ok(header) => header,

        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let _ =
        manejo_de_csv::actualizar_csv(ruta_csv, header, campos_para_actualizar, donde_actualizar);
}

///Funcion que se encarga de manejar la consulta "UPDATE"
/// Recibe la consulta y la ruta del archivo y llama a las demas funciones para procesarlos y realizar el delete
pub fn delete(consulta_sql: String, ruta_del_archivo: String) {
    let (nombre_del_csv, clave) = match manejo_de_string::separar_datos_delete(consulta_sql) {
        Ok((nombre_del_csv, clave)) => (nombre_del_csv, clave),

        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let ruta_csv = manejo_de_csv::obtener_ruta_del_csv(ruta_del_archivo, &nombre_del_csv);

    let header = match manejo_de_csv::leer_header(&ruta_csv) {
        Ok(header) => header,

        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let _ = manejo_de_csv::borrar_lineas_csv(ruta_csv, header, clave);
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
pub fn select(consulta_sql: String, ruta_del_archivo: String) {
    let (nombre_csv, columnas, condiciones) =
        match manejo_de_string::separar_datos_select(consulta_sql) {
            Ok((nombre_csv, columnas, condiciones)) => (nombre_csv, columnas, condiciones),

            Err(e) => {
                println!("{}", e);
                return;
            }
        };

    let (condiciones, ordenamiento) = manejo_de_string::separar_order(condiciones);
    let condiciones_parseadas = condiciones::procesar_condiciones(condiciones);
    let ruta_csv = manejo_de_csv::obtener_ruta_del_csv(ruta_del_archivo, &nombre_csv);

    let header = match manejo_de_csv::leer_header(&ruta_csv) {
        Ok(header) => header,

        Err(e) => {
            println!("{}", e);
            return;
        }
    };

    let matriz =
        match condiciones::comparar_con_csv(condiciones_parseadas, ruta_csv, header.clone()) {
            Ok(matriz) => matriz,

            Err(e) => {
                println!("{}", e);
                return;
            }
        };

    mostrar_select(matriz, columnas, &header, ordenamiento);
}
