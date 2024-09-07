//La compilación no debe arrojar warnings del compilador, ni del linter clippy.
mod manejo_de_string;
mod manejo_de_csv;
mod consultas;
mod condiciones;

fn realizar_consulta(consulta_sql:String ,ruta: String) {
    
    
    if manejo_de_string::obtener_primera_palabra(&consulta_sql)== "INSERT" {

        consultas::insert(consulta_sql, ruta);
    }

    else if manejo_de_string::obtener_primera_palabra(&consulta_sql) == "UPDATE"  {

        consultas::update(consulta_sql,ruta);
    }

    else if manejo_de_string::obtener_primera_palabra(&consulta_sql) == "DELETE"{
        consultas::delete(consulta_sql,ruta);
        
    }

    else if manejo_de_string::obtener_primera_palabra(&consulta_sql) == "SELECT"{
        consultas::select(consulta_sql,ruta);
    }


    else {
        println!("No existe la consulta escrita!")
    }
    
}


fn main() {
   
    let consulta_completa: Vec<String> = std::env::args().collect();

    let ruta = &consulta_completa[1];  
    let consulta_sql: &String = &consulta_completa[2];  

   
    realizar_consulta(consulta_sql.to_string(),ruta.to_string())

}
