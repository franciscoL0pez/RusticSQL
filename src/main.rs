//La compilación no debe arrojar warnings del compilador, ni del linter clippy.
mod condiciones;
mod consultas;
mod errors;
mod manejo_de_csv;
mod manejo_de_string;
mod tipo_de_datos;
use crate::consultas::delete::delete;
use crate::consultas::insert::insert;
use crate::consultas::select::select;
use crate::consultas::update::update;

//quiero importar las consultas de mi carpeta consultas, que tiene insert,delete,select,update la
//cual me ayudara a realizar las consultas

fn realizar_consulta(consulta_sql: &str, ruta: &str) -> Result<(), errors::SqlError> {
    // Obtener la primera palabra solo una vez
    let primera_palabra = match manejo_de_string::obtener_primera_palabra(&consulta_sql) {
        Ok(palabra) => palabra,
        Err(e) => {
            eprintln!("{}", e);
            return Err(errors::SqlError::InvalidSyntax);
        }
    };

    match primera_palabra.as_str() {
        "INSERT" => insert(consulta_sql, ruta)?,
        "UPDATE" => update(consulta_sql, ruta)?,
        "DELETE" => delete(consulta_sql, ruta)?,
        "SELECT" => select(consulta_sql, ruta)?,
        _ => return Err(errors::SqlError::InvalidSyntax),
    }

    Ok(())
}
///Recibe una consulta por consolta y la recollecta en un vector
///Luego la divide en dos vectores y llama al manejador de consultas para realizar la consulta
fn main() {
    let consulta_completa: Vec<String> = std::env::args().collect();

    let ruta = &consulta_completa[1];
    println!("{}", ruta);
    let consulta_sql: &String = &consulta_completa[2];
    println!("{}", consulta_sql);
    match realizar_consulta(consulta_sql, ruta) {
        Ok(_) => (),
        Err(e) => eprintln!("{}", e),
    }
}
