use axum::{
    Json, Router,
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode},
    response::Html,
    routing::get,
};
use deoos_engine::Engine;
use serde_json::Value;
#[derive(Clone)]
struct Server {
    engine: Engine,
    token: Option<String>,
}
async fn dispatch_http(
    State(server): State<Server>,
    headers: HeaderMap,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> Result<Json<Value>, (StatusCode, String)> {
    // Browser requests must originate at this server. SDKs do not send Origin.
    if let Some(origin) = headers.get("origin") {
        let origin = origin
            .to_str()
            .ok()
            .and_then(|value| value.parse::<axum::http::Uri>().ok());
        let host = headers.get("host").and_then(|value| value.to_str().ok());
        let same_origin = origin.as_ref().is_some_and(|origin| {
            matches!(origin.scheme_str(), Some("http" | "https"))
                && origin.query().is_none()
                && origin.path() == "/"
                && origin
                    .authority()
                    .zip(host)
                    .is_some_and(|(authority, host)| authority.as_str().eq_ignore_ascii_case(host))
        });
        if !same_origin {
            return Err((
                StatusCode::FORBIDDEN,
                "cross-origin request rejected".into(),
            ));
        }
    }
    if method == Method::POST
        && !headers
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case("application/json")
            })
    {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "application/json required".into(),
        ));
    }
    if let Some(token) = &server.token
        && headers.get("authorization").and_then(|v| v.to_str().ok())
            != Some(format!("Bearer {token}").as_str())
    {
        return Err((StatusCode::UNAUTHORIZED, "authentication required".into()));
    }
    let data = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body)
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid JSON".into()))?
    };
    server
        .engine
        .dispatch(method.as_str(), uri.path(), data)
        .await
        .map(Json)
}
async fn ui() -> impl axum::response::IntoResponse {
    (
        [
            (
                "content-security-policy",
                "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
            ),
            ("cache-control", "no-store"),
            ("x-content-type-options", "nosniff"),
        ],
        Html(include_str!("ui.html")),
    )
}
#[tokio::main]
async fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("--version") => {
            println!("deoos-server {}", env!("CARGO_PKG_VERSION"));
            return;
        }
        Some("--check-storage") => {
            let result = match Engine::from_env() {
                Ok(engine) => engine.check_storage().await,
                Err(error) => Err(error),
            };
            match result {
                Ok(report) => println!("{}", serde_json::to_string_pretty(&report).unwrap()),
                Err(error) => {
                    eprintln!("storage qualification failed: {error}");
                    std::process::exit(1);
                }
            }
            return;
        }
        Some("--help") | Some("-h") => {
            println!(
                "deoos-server: object-storage-backed execution service\n\nConfiguration via environment:\n  DEOOS_STORAGE_PROVIDER: s3 (default), gcs, azure or filesystem\n  DEOOS_STORAGE_DIRECTORY: required for filesystem storage\n    Experimental macOS/APFS backend; qualified on Apple Silicon\n  DEOOS_STORAGE_BUCKET: bucket/container\n  GOOGLE_* or AZURE_* native credentials for their providers\n  AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY for S3\n  AWS_SESSION_TOKEN for temporary credentials\n  AWS_ENDPOINT and AWS_ALLOW_HTTP=true for local S3-compatible storage\n  EXECUTION_PREFIX (default deoos)\n  ENGINE_BIND (default 127.0.0.1:7331)\n  ENGINE_TOKEN required for non-loopback server-mode access\n  LEASE_MS (default 30000)\n\nUse the Python or TypeScript SDK to submit and execute tasks.\n--check-storage tests conditional writes and read/list consistency in an isolated temporary namespace."
            );
            return;
        }
        Some(_) => {
            eprintln!("unknown argument; use --help");
            std::process::exit(2);
        }
        None => {}
    }
    let e = Engine::from_env().expect("storage configuration");
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/", get(ui))
        .route("/ui", get(ui))
        .fallback(dispatch_http)
        .with_state(Server {
            engine: e,
            token: std::env::var("ENGINE_TOKEN").ok(),
        });
    let addr = std::env::var("ENGINE_BIND").unwrap_or("127.0.0.1:7331".into());
    let socket: std::net::SocketAddr = addr.parse().expect("ENGINE_BIND must be an IP:port");
    if !socket.ip().is_loopback() {
        assert!(
            !std::env::var("ENGINE_TOKEN").unwrap_or_default().is_empty(),
            "ENGINE_TOKEN required for non-loopback server mode"
        );
    }
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    eprintln!("deoos-server listening {addr}");
    axum::serve(listener, app).await.unwrap();
}
