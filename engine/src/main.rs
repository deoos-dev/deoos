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
            return Err(deoos_engine::error_guide(
                StatusCode::FORBIDDEN,
                "CROSS_ORIGIN",
                "cross-origin request rejected",
                "the browser Origin header does not match this server's Host",
                "open the UI at this server's origin, for example http://127.0.0.1:7331/ui. SDK requests must not send an Origin header",
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
        return Err(deoos_engine::error_guide(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "INVALID_CONTENT_TYPE",
            "application/json required; set Content-Type to application/json",
            "this POST did not send a JSON body",
            "set the Content-Type header to application/json. The Python and TypeScript SDKs do this",
        ));
    }
    if let Some(token) = &server.token
        && headers.get("authorization").and_then(|v| v.to_str().ok())
            != Some(format!("Bearer {token}").as_str())
    {
        return Err(deoos_engine::error_guide(
            StatusCode::UNAUTHORIZED,
            "AUTH_REQUIRED",
            "authentication required; set ENGINE_TOKEN to the token this server was started with",
            "this server was started with ENGINE_TOKEN and the request did not send that bearer token",
            "export ENGINE_TOKEN to the same value and pass it to Client.remote(url, token). `make dev` leaves ENGINE_TOKEN unset and binds to loopback",
        ));
    }
    let data = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body)
            .map_err(|_| {
                deoos_engine::error_guide(
                    StatusCode::BAD_REQUEST,
                    "INVALID_JSON",
                    "invalid JSON; send a JSON object, for example {\"id\":\"task-1\",\"handler\":\"greet\",\"inputs\":{}}",
                    "the request body is not JSON",
                    "send a JSON object with Content-Type application/json. Strings in the CLI must be quoted, for example '{\"ok\": true}'",
                )
            })?
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
                    let message = if error
                        .contains("https://github.com/hckshr/deoos/blob/main/docs/errors.md#")
                    {
                        error
                    } else {
                        deoos_engine::error_text(
                            "STORAGE_CHECK_FAILED",
                            &format!("storage qualification failed: {error}"),
                            "the bucket did not provide conditional writes and read-after-write, or the store could not be reached",
                            "check DEOOS_STORAGE_BUCKET, AWS_ENDPOINT, AWS_ALLOW_HTTP, AWS_REGION, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY. For local RustFS run `docker compose up -d` and `make dev`",
                        )
                    };
                    eprintln!("{message}");
                    std::process::exit(1);
                }
            }
            return;
        }
        Some("--help") | Some("-h") => {
            println!(
                "deoos-server: object-storage-backed execution service\n\nConfiguration via environment:\n  DEOOS_STORAGE_PROVIDER: s3 (default), gcs, azure or filesystem\n  DEOOS_STORAGE_DIRECTORY: required for filesystem storage\n    Experimental macOS/APFS backend; qualified on Apple Silicon\n  DEOOS_STORAGE_BUCKET: bucket/container\n  GOOGLE_* or AZURE_* native credentials for their providers\n  AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY for S3\n  AWS_SESSION_TOKEN for temporary credentials\n  AWS_ENDPOINT and AWS_ALLOW_HTTP=true for local S3-compatible storage\n  EXECUTION_PREFIX (default deoos)\n  ENGINE_BIND (default 127.0.0.1:7331)\n  ENGINE_TOKEN required for non-loopback server-mode access\n  LEASE_MS (default 30000)\n\nUse the Python or TypeScript SDK to submit and execute tasks.\n--check-storage tests conditional writes and read/list consistency in an isolated temporary namespace.\nError codes: https://github.com/hckshr/deoos/blob/main/docs/errors.md and docs/errors.md in a checkout."
            );
            return;
        }
        Some(_) => {
            eprintln!(
                "{}",
                deoos_engine::error_text(
                    "SERVER_ARGUMENT",
                    "unknown argument",
                    "deoos-server accepts only --help, --version, and --check-storage",
                    "run `deoos-server --help`",
                )
            );
            std::process::exit(2);
        }
        None => {}
    }
    let e = match Engine::from_env() {
        Ok(engine) => engine,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    };
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
    let socket: std::net::SocketAddr = match addr.parse() {
        Ok(socket) => socket,
        Err(_) => {
            eprintln!(
                "{}",
                deoos_engine::error_text(
                    "BIND_INVALID",
                    &format!("ENGINE_BIND '{addr}' is not an IP:port"),
                    "ENGINE_BIND must be an address and port the process can bind",
                    "set ENGINE_BIND to an IP and port, for example 127.0.0.1:7331",
                )
            );
            std::process::exit(1);
        }
    };
    if !socket.ip().is_loopback() && std::env::var("ENGINE_TOKEN").unwrap_or_default().is_empty() {
        eprintln!(
            "{}",
            deoos_engine::error_text(
                "TOKEN_REQUIRED",
                "ENGINE_TOKEN required for non-loopback server mode",
                "a server bound to a non-loopback address accepts remote callers",
                "set ENGINE_TOKEN to a long random secret, or set ENGINE_BIND to a loopback address such as 127.0.0.1:7331",
            )
        );
        std::process::exit(1);
    }
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!(
                "{}",
                deoos_engine::error_text(
                    "BIND_IN_USE",
                    &format!("cannot listen on {addr}: {error}"),
                    "the address is already in use or is not available to this process",
                    "stop the other process or set ENGINE_BIND to a free IP:port. `make dev` uses 127.0.0.1:7331",
                )
            );
            std::process::exit(1);
        }
    };
    eprintln!("deoos-server listening {addr}");
    axum::serve(listener, app).await.unwrap();
}
