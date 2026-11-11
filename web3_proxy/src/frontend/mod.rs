//! `frontend` contains HTTP and websocket endpoints for use by a website or web3 wallet.
//!
//! Important reading about axum extractors: <https://docs.rs/axum/latest/axum/extract/index.html#the-order-of-extractors>
//!
//! There are a lot of things in tower/axum that i should have used instead of implementing here.
// TODO: these are only public so docs are generated. What's a better way to do this?
pub mod errors;
mod request_body;
pub mod request_id;
pub mod rpc_proxy_http;
pub mod rpc_proxy_ws;
pub mod status;

use crate::app::App;
use crate::errors::Web3ProxyResult;
use axum::{
    body::Body,
    routing::{get, post},
    Router,
};
use http::Request;
use request_id::RequestId;

use std::sync::Arc;
use std::{net::SocketAddr, sync::atomic::Ordering};
use tokio::{
    net::{TcpListener, TcpSocket},
    process::Command,
};
use tower_http::{cors::CorsLayer, normalize_path::NormalizePathLayer, trace::TraceLayer};
use tracing::{error, error_span, info, trace_span};

#[cfg(feature = "listenfd")]
use listenfd::ListenFd;

const LISTEN_BACKLOG: u32 = i32::MAX as u32;

fn bind_tcp_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    info!(%addr, requested_backlog = LISTEN_BACKLOG, "binding TCP listener");

    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(LISTEN_BACKLOG)
}

/// build our axum Router
pub fn make_router(app: Arc<App>) -> Router<()> {
    let router = Router::<Arc<App>>::new()
        // TODO: i think these routes could be done a lot better
        //
        // HTTP RPC (POST)
        //
        // Websocket RPC (GET)
        // If not an RPC, GET will redirect to urls in the config
        //
        // public
        .route(
            "/",
            post(rpc_proxy_http::proxy_web3_rpc).get(rpc_proxy_ws::websocket_handler),
        )
        // public fastest
        .route(
            "/fastest",
            post(rpc_proxy_http::fastest_proxy_web3_rpc)
                .get(rpc_proxy_ws::fastest_websocket_handler),
        )
        .route(
            "/fastest/",
            post(rpc_proxy_http::fastest_proxy_web3_rpc)
                .get(rpc_proxy_ws::fastest_websocket_handler),
        )
        // public versus
        .route(
            "/versus",
            post(rpc_proxy_http::versus_proxy_web3_rpc).get(rpc_proxy_ws::versus_websocket_handler),
        )
        .route(
            "/versus/",
            post(rpc_proxy_http::versus_proxy_web3_rpc).get(rpc_proxy_ws::versus_websocket_handler),
        )
        //
        // System things
        //
        .route("/health", get(status::health))
        .route("/status", get(status::status))
        .route("/status/backups_needed", get(status::backups_needed))
        .route("/status/debug_request", get(status::debug_request));

    with_layers(router)
        // 404 for any unknown routes
        .fallback(errors::handler_404)
        .with_state(app)
}

fn with_layers<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    // Tuple layers run in request order. Compose them before Axum boxes the route.
    request_body::with_body_limits(router).layer((
        request_id::RequestIdLayer,
        TraceLayer::new_for_http().make_span_with(|request: &Request<Body>| {
            // We get the request id from the header
            // If no header, a new Ulid is created
            // TODO: move this header name to config
            /*
            let request_id = request
                .headers()
                .get("x-amzn-trace-id")
                .and_then(|x| x.to_str().ok())
                .map(ToString::to_string)
                .unwrap_or_else(|| Ulid::new().to_string());
            */
            let request_id = &request.extensions().get::<RequestId>().unwrap().0;

            // And then we put it along with other information into the `request` span
            // TODO: what other info should we attach? how can we attach an error and a tracing span here?
            // TODO: how can we do a tracing_span OR an error_span?
            let s = trace_span!(
                "request",
                id = %request_id,
                method = %request.method(),
                path = %request.uri().path(),
            );

            if s.is_disabled() {
                error_span!(
                    "request",
                    id = %request_id,
                )
            } else {
                s
            }
        }),
        CorsLayer::very_permissive(),
        NormalizePathLayer::trim_trailing_slash(),
    ))
}

/// Start the frontend server.
pub async fn serve(app: Arc<App>) -> Web3ProxyResult<()> {
    let mut shutdown_receiver = app.frontend_shutdown.subscribe();
    // TODO: read config for if fastest/versus should be available publicly. default off
    let router = make_router(app.clone());

    // TODO: https://docs.rs/tower-http/latest/tower_http/propagate_header/index.html

    #[cfg(feature = "listenfd")]
    let listener = if let Some(listener) = ListenFd::from_env().take_tcp_listener(0)? {
        // use systemd socket magic for no downtime deploys
        let addr = listener.local_addr()?;

        info!("listening with fd at {}", addr);

        listener.set_nonblocking(true)?;
        TcpListener::from_std(listener)?
    } else {
        // TODO: allow only listening on localhost? top_config.app.host.parse()?
        let addr = SocketAddr::from(([0, 0, 0, 0], app.frontend_port.load(Ordering::SeqCst)));

        bind_tcp_listener(addr)?
    };
    #[cfg(not(feature = "listenfd"))]
    let listener = {
        let addr = SocketAddr::from(([0, 0, 0, 0], app.frontend_port.load(Ordering::SeqCst)));

        bind_tcp_listener(addr)?
    };

    // The frontend runs behind a trusted proxy. Client IP extraction uses the
    // rightmost address from X-Forwarded-For.
    let make_service = router.into_make_service();

    let server = axum::serve(listener, make_service);

    let port = server.local_addr()?.port();
    info!("listening on port {}", port);

    app.frontend_port.store(port, Ordering::SeqCst);

    let shutdown_app = app.clone();
    let result = server
        // TODO: option to use with_connect_info. we want it in dev, but not when running behind a proxy, but not
        .with_graceful_shutdown(async move {
            let _ = shutdown_receiver.wait_for(|stopping| *stopping).await;

            if let Some(shutdown_script) = shutdown_app.config.shutdown_script.as_ref() {
                let shutdown_script = Command::new(shutdown_script)
                    .args(&shutdown_app.config.shutdown_script_args)
                    .spawn()
                    .expect("failed to execute script");

                match shutdown_script.wait_with_output().await {
                    Ok(x) => {
                        info!(?x, "shutdown script finished");
                    }
                    Err(err) => {
                        error!(?err, "shutdown script failed");
                    }
                };
            }
        })
        .await
        .map_err(Into::into);
    app.frontend_shutdown.send_replace(true);
    app.frontend_tasks.close();
    tokio::time::timeout(
        std::time::Duration::from_secs(23),
        app.frontend_tasks.wait(),
    )
    .await?;
    result
}

#[cfg(test)]
mod tests {
    use super::bind_tcp_listener;
    use std::net::SocketAddr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[tokio::test]
    async fn bound_tcp_listener_accepts_connections() {
        let listener = bind_tcp_listener(SocketAddr::from(([127, 0, 0, 1], 0))).unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client = TcpStream::connect(addr)
            .await
            .unwrap_or_else(|err| panic!("failed to connect to {addr}: {err}"));
        let (mut server, _) = listener.accept().await.unwrap();

        client.write_all(b"ready").await.unwrap();

        let mut received = [0; 5];
        server.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"ready");
    }
}

#[cfg(test)]
mod middleware_tests {
    use super::{with_layers, RequestId};
    use axum::{
        body::{to_bytes, Body, Bytes},
        extract::Extension,
        http::{header, Request, StatusCode, Uri},
        routing::post,
        Router,
    };
    use tower_service::Service;

    async fn echo(Extension(id): Extension<RequestId>, uri: Uri, body: Bytes) -> String {
        format!(
            "{}|{}|{}",
            id.0,
            uri.path(),
            String::from_utf8(body.to_vec()).unwrap()
        )
    }

    fn router() -> Router {
        with_layers(
            Router::new()
                .route("/probe", post(echo))
                .route("/probe/", post(echo)),
        )
    }

    #[tokio::test]
    async fn request_id_and_normalized_path_reach_handler() {
        let response = router()
            .call(
                Request::post("/probe/")
                    .header("x-amzn-trace-id", "known-id")
                    .header(header::ORIGIN, "https://client.example")
                    .body(Body::from("payload"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap(),
            "known-id|/probe|payload"
        );

        let response = router()
            .call(Request::post("/probe").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let (id, rest) = body.split_once('|').unwrap();
        ulid::Ulid::from_string(id).unwrap();
        assert_eq!(rest, "/probe|");
    }

    #[tokio::test]
    async fn cors_preflight_precedes_body_decoding() {
        let response = router()
            .call(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/probe")
                    .header(header::ORIGIN, "https://client.example")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(header::CONTENT_ENCODING, "unsupported")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_METHODS],
            "POST"
        );
        assert!(to_bytes(response.into_body(), 1024)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn encoding_errors_keep_cors_headers() {
        let response = router()
            .call(
                Request::post("/probe")
                    .header(header::ORIGIN, "https://client.example")
                    .header(header::CONTENT_ENCODING, "unsupported")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(
            response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
            "https://client.example"
        );
        assert_eq!(
            response.headers()[header::ACCEPT_ENCODING],
            "gzip, identity"
        );
    }
}
