use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
static _LOCK: AtomicBool = AtomicBool::new(false);

static _TEST_ID: AtomicUsize = AtomicUsize::new(0);

pub fn _archivo_temp(nombre: &str) -> String {
    let id = _TEST_ID.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}.csv", nombre, id)
}

pub fn _acquire_lock() {
    while _LOCK.swap(true, Ordering::Acquire) {}
}

pub fn _release_lock() {
    _LOCK.store(false, Ordering::Release);
}
