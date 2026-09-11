use super::BlockRelay;
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use std::sync::Arc;

impl BlockRelay {
    /// Keep this listener private. It contains target names and operational measurements.
    pub async fn serve_status(
        self: Arc<Self>,
        listener: tokio::net::TcpListener,
        mut stop: tokio::sync::broadcast::Receiver<()>,
    ) -> std::io::Result<()> {
        axum::serve(listener, self.router())
            .with_graceful_shutdown(async move {
                let _ = stop.recv().await;
            })
            .await
    }
    fn router(self: Arc<Self>) -> Router {
        Router::new()
            .route(
                "/live",
                get(|State(relay): State<Arc<BlockRelay>>| async move {
                    if relay.live() {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }),
            )
            .route(
                "/health",
                get(|State(relay): State<Arc<BlockRelay>>| async move {
                    if relay.ready() {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }),
            )
            .route(
                "/status",
                get(|State(relay): State<Arc<BlockRelay>>| async move {
                    (
                        [(header::CONTENT_TYPE, "application/json")],
                        relay.snapshot().to_string(),
                    )
                        .into_response()
                }),
            )
            .with_state(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn live_requires_running_supervisor_and_health_requires_forwarding() {
        let relay = BlockRelay::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, _) = tokio::sync::broadcast::channel(1);
        let server = tokio::spawn(relay.clone().serve_status(listener, stop.subscribe()));
        let client = super::super::transport::client().unwrap();
        assert_eq!(
            client
                .get(format!("{url}/live"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let run = tokio::spawn(relay.clone().run(1, stop.subscribe()));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !relay.live() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            client
                .get(format!("{url}/live"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .get(format!("{url}/health"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            client
                .post(format!("{url}/status"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        run.await.unwrap();
        assert!(!relay.live());
    }
}
