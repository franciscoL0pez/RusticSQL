use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
static LOCK: AtomicBool = AtomicBool::new(false);

static TEST_ID: AtomicUsize = AtomicUsize::new(0);

pub fn archivo_temp(nombre: &str) -> String {
    let id = TEST_ID.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}.csv", nombre, id)
}


pub fn acquire_lock() {
    while LOCK.swap(true, Ordering::Acquire) {
       
    }
}

pub fn release_lock() {
    LOCK.store(false, Ordering::Release);
}
