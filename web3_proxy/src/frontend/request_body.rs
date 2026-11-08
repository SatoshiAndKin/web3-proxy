//! Decode HTTP request content without bypassing the wire or decoded body limits.

use async_compression::tokio::bufread::GzipDecoder;
use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Request},
    http::{header, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    Router,
};
use futures_util::TryStreamExt;
use tokio_util::io::{ReaderStream, StreamReader};
use tower_http::limit::RequestBodyLimitLayer;

const MAX_HTTP_REQUEST_BODY_SIZE: usize = 32 * 1024 * 1024;

pub(super) fn with_body_limits<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        // Bytes extractors apply this limit to the decoded body.
        .layer(DefaultBodyLimit::max(MAX_HTTP_REQUEST_BODY_SIZE))
        .layer(middleware::from_fn(decode_gzip))
        // This outer limit also bounds encoded/chunked input.
        .layer(RequestBodyLimitLayer::new(MAX_HTTP_REQUEST_BODY_SIZE))
}

async fn decode_gzip(request: Request, next: Next) -> Response {
    let mut encodings = request.headers().get_all(header::CONTENT_ENCODING).iter();
    let Some(encoding) = encodings.next() else {
        return next.run(request).await;
    };
    let encoding = encoding.to_str().unwrap_or_default().trim();
    if encodings.next().is_some() {
        return unsupported_encoding();
    }
    if encoding.eq_ignore_ascii_case("identity") {
        return next.run(request).await;
    }
    if !encoding.eq_ignore_ascii_case("gzip") {
        return unsupported_encoding();
    }

    let (mut parts, body) = request.into_parts();
    let reader = StreamReader::new(body.into_data_stream().map_err(std::io::Error::other));
    let mut decoder = GzipDecoder::new(reader);
    // Gzip permits concatenated members. Consume all members and validate their
    // trailers so a valid prefix cannot hide additional or damaged request data.
    decoder.multiple_members(true);
    parts.headers.remove(header::CONTENT_ENCODING);
    parts.headers.remove(header::CONTENT_LENGTH);
    let body = Body::from_stream(ReaderStream::new(decoder));
    next.run(Request::from_parts(parts, body)).await
}

fn unsupported_encoding() -> Response {
    (
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        [(header::ACCEPT_ENCODING, "gzip, identity")],
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::{with_body_limits, MAX_HTTP_REQUEST_BODY_SIZE};
    use axum::{
        body::{to_bytes, Body, Bytes},
        http::{header, Request, StatusCode},
        routing::post,
        Router,
    };
    use flate2::{write::GzEncoder, Compression};
    use futures_util::stream;
    use std::{convert::Infallible, io::Write};
    use tower_service::Service;

    fn gzip(body: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(body).unwrap();
        encoder.finish().unwrap()
    }

    async fn echo(request: Request<Body>) -> axum::response::Response {
        let mut router =
            with_body_limits(Router::new().route("/", post(|body: Bytes| async { body })));
        router.call(request).await.unwrap()
    }

    async fn encoded(body: Vec<u8>) -> axum::response::Response {
        echo(
            Request::post("/")
                .header(header::CONTENT_ENCODING, "gzip")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
    }

    #[tokio::test]
    async fn plain_and_identity_requests_preserve_bytes() {
        for encoding in [None, Some("identity")] {
            let mut request = Request::post("/");
            if let Some(encoding) = encoding {
                request = request.header(header::CONTENT_ENCODING, encoding);
            }
            let response = echo(request.body(Body::from("unchanged")).unwrap()).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(response.into_body(), 100).await.unwrap(),
                "unchanged"
            );
        }
    }

    #[tokio::test]
    async fn gzip_and_concatenated_members_preserve_all_bytes() {
        let expected = br#"{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}"#;
        let split = expected.len() / 2;
        for body in [
            gzip(expected),
            [gzip(&expected[..split]), gzip(&expected[split..])].concat(),
        ] {
            let response = encoded(body).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                to_bytes(response.into_body(), 1000).await.unwrap().as_ref(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn streamed_gzip_decodes_without_content_length() {
        let chunks = gzip(b"streamed body")
            .into_iter()
            .map(|byte| Ok::<_, Infallible>(Bytes::from(vec![byte])))
            .collect::<Vec<_>>();
        let response = echo(
            Request::post("/")
                .header(header::CONTENT_ENCODING, "gzip")
                .body(Body::from_stream(stream::iter(chunks)))
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), 100).await.unwrap(),
            "streamed body"
        );
    }

    #[tokio::test]
    async fn damaged_or_incomplete_gzip_is_rejected() {
        let valid = gzip(b"request");
        let mut bad_crc = valid.clone();
        let trailer = bad_crc.len() - 8;
        bad_crc[trailer] ^= 1;
        for body in [
            valid[..valid.len() - 1].to_vec(),
            bad_crc,
            [valid.clone(), b"trailing junk".to_vec()].concat(),
            [valid, gzip(b"second")[..5].to_vec()].concat(),
        ] {
            assert_eq!(encoded(body).await.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn unsupported_or_stacked_content_encoding_is_rejected() {
        for encodings in [vec!["br"], vec!["gzip, gzip"], vec!["gzip", "identity"]] {
            let mut request = Request::post("/");
            for encoding in encodings {
                request = request.header(header::CONTENT_ENCODING, encoding);
            }
            let response = echo(request.body(Body::from(gzip(b"request"))).unwrap()).await;
            assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
            assert_eq!(
                response.headers()[header::ACCEPT_ENCODING],
                "gzip, identity"
            );
        }
    }

    #[tokio::test]
    async fn plain_body_limit_accepts_32_mib_and_rejects_more() {
        for (size, status) in [
            (MAX_HTTP_REQUEST_BODY_SIZE, StatusCode::OK),
            (
                MAX_HTTP_REQUEST_BODY_SIZE + 1,
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ] {
            let response = echo(Request::post("/").body(Body::from(vec![0; size])).unwrap()).await;
            assert_eq!(response.status(), status);
        }
    }

    #[tokio::test]
    async fn decoded_body_limit_accepts_32_mib_and_rejects_more() {
        for (size, status) in [
            (MAX_HTTP_REQUEST_BODY_SIZE, StatusCode::OK),
            (
                MAX_HTTP_REQUEST_BODY_SIZE + 1,
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
        ] {
            assert_eq!(encoded(gzip(&vec![0; size])).await.status(), status);
        }
    }

    #[tokio::test]
    async fn encoded_content_length_cannot_bypass_wire_limit() {
        let response = echo(
            Request::post("/")
                .header(header::CONTENT_ENCODING, "gzip")
                .header(header::CONTENT_LENGTH, MAX_HTTP_REQUEST_BODY_SIZE + 1)
                .body(Body::from(gzip(b"small decoded body")))
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
