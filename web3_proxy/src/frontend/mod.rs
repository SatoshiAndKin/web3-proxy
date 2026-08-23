//! `frontend` contains HTTP and websocket endpoints for use by a website or web3 wallet.
//!
//! Important reading about axum extractors: <https://docs.rs/axum/latest/axum/extract/index.html#the-order-of-extractors>
//!
//! There are a lot of things in tower/axum that i should have used instead of implementing here.
// TODO: these are only public so docs are generated. What's a better way to do this?
pub mod errors;
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
    sync::broadcast,
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

    // Axum layers
    // layers are ordered bottom up
    // the last layer is first for requests and last for responses
    let router: Router = router
        // Remove trailing slashes
        // TODO: this isn't working for me. why?
        .layer(NormalizePathLayer::trim_trailing_slash())
        // handle cors. we expect queries from all sorts of places
        .layer(CorsLayer::very_permissive())
        // request id
        .layer(
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
            }), // .on_failure(|| todo!("on failure that has the request and response body so we can debug more easily")),
        )
        .layer(request_id::RequestIdLayer)
        // 404 for any unknown routes
        .fallback(errors::handler_404)
        .with_state(app);

    router
}

/// Start the frontend server.
pub async fn serve(
    app: Arc<App>,
    mut shutdown_receiver: broadcast::Receiver<()>,
    shutdown_complete_sender: broadcast::Sender<()>,
) -> Web3ProxyResult<()> {
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

    let server = server
        // TODO: option to use with_connect_info. we want it in dev, but not when running behind a proxy, but not
        .with_graceful_shutdown(async move {
            let _ = shutdown_receiver.recv().await;

            if let Some(shutdown_script) = app.config.shutdown_script.as_ref() {
                let shutdown_script = Command::new(shutdown_script)
                    .args(&app.config.shutdown_script_args)
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

    let _ = shutdown_complete_sender.send(());

    server
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
