//! In-process C ABI used by the Python SDK. No process or network listener is started.
use super::{Config, Engine};
use serde_json::{Value, json};
use std::ffi::{CStr, CString, c_char, c_void};
struct Handle {
    engine: Engine,
    runtime: tokio::runtime::Runtime,
}
fn output(value: Value) -> *mut c_char {
    CString::new(value.to_string()).unwrap().into_raw()
}
unsafe fn string<'a>(p: *const c_char) -> Result<&'a str, String> {
    if p.is_null() {
        return Err("null string".into());
    }
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|e| e.to_string())
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn deoos_open(config: *const c_char, error: *mut *mut c_char) -> *mut c_void {
    let result = std::panic::catch_unwind(|| -> Result<Handle, String> {
        let c: Config =
            serde_json::from_str(unsafe { string(config) }?).map_err(|e| e.to_string())?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let _entered = runtime.enter();
        let engine = Engine::from_config(c)?;
        drop(_entered);
        Ok(Handle { engine, runtime })
    });
    match result {
        Ok(Ok(h)) => Box::into_raw(Box::new(h)).cast(),
        other => {
            let message = match other {
                Ok(Err(e)) => e,
                _ => "native initialization panicked".into(),
            };
            if !error.is_null() {
                unsafe {
                    *error = output(json!({"error":message}));
                }
            }
            std::ptr::null_mut()
        }
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn deoos_request(handle: *mut c_void, request: *const c_char) -> *mut c_char {
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<Value, String> {
            if handle.is_null() {
                return Err("closed engine".into());
            }
            let h = unsafe { &*handle.cast::<Handle>() };
            let request: Value =
                serde_json::from_str(unsafe { string(request) }?).map_err(|e| e.to_string())?;
            let method = request["method"].as_str().ok_or("method required")?;
            let path = request["path"].as_str().ok_or("path required")?;
            Ok(
                match h
                    .runtime
                    .block_on(h.engine.dispatch(method, path, request["data"].clone()))
                {
                    Ok(value) => json!({"status":200,"value":value}),
                    Err((code, message)) => json!({"status":code.as_u16(),"error":message}),
                },
            )
        }));
    output(match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({"status":500,"error":e}),
        Err(_) => json!({"status":500,"error":"native request panicked"}),
    })
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn deoos_close(handle: *mut c_void) {
    if !handle.is_null() {
        drop(unsafe { Box::from_raw(handle.cast::<Handle>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn deoos_string_free(value: *mut c_char) {
    if !value.is_null() {
        drop(unsafe { CString::from_raw(value) });
    }
}
