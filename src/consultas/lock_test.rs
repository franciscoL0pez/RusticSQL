use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
static _LOCK: AtomicBool = AtomicBool::new(false);

static _TEST_ID: AtomicUsize = AtomicUsize::new(0);
/// -------Funciones especificas para no tener problemas al correr los test ---------

/// Funcion para generar un nombre de archivo temporal
/// #Recibe el nombre del archivo y le agrega un numero de test
/// -Incrementa el contador de test
/// -Retorna el nombre del archivo con el numero de test
///
pub fn _archivo_temp(nombre: &str) -> String {
    let id = _TEST_ID.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}.csv", nombre, id)
}

/// Funcion para adquirir el lock
/// -Mientras el lock este en true, se mantiene en el loop
/// -Cuando el lock esta en false, lo cambia a true
pub fn _acquire_lock() {
    while _LOCK.swap(true, Ordering::Acquire) {}
}

/// Funcion para liberar el lock
/// -Cambia el lock a false
/// -Se utiliza para liberar el lock
pub fn _release_lock() {
    _LOCK.store(false, Ordering::Release);
}
