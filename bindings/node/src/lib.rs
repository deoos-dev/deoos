use deoos_engine::{Config, Engine};
use napi_derive::napi;
use std::sync::Arc;
#[napi]
pub struct NativeEngine {
    engine: Arc<Engine>,
}
#[napi]
impl NativeEngine {
    #[napi(constructor)]
    pub fn new(config: String) -> napi::Result<Self> {
        let config: Config =
            serde_json::from_str(&config).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(Self {
            engine: Arc::new(Engine::from_config(config).map_err(napi::Error::from_reason)?),
        })
    }
    #[napi]
    pub async fn request(
        &self,
        method: String,
        path: String,
        data: String,
    ) -> napi::Result<String> {
        let value =
            serde_json::from_str(&data).map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(match self.engine.dispatch(&method, &path, value).await {
            Ok(v) => serde_json::json!({"status":200,"value":v}).to_string(),
            Err((code, message)) => {
                serde_json::json!({"status":code.as_u16(),"error":message}).to_string()
            }
        })
    }
}
