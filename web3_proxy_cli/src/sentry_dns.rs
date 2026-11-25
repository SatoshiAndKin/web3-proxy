use hickory_resolver::{
    net::{runtime::TokioRuntimeProvider, NetError},
    ResolverBuilder, TokioResolver,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::OnceCell;
use web3_proxy::prelude::{reqwest, tokio};

/// Keep asynchronous DNS local to Sentry; ordinary application clients are unchanged.
#[derive(Default)]
pub(super) struct SentryDns(Arc<OnceCell<TokioResolver>>);

impl reqwest::dns::Resolve for SentryDns {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let state = self.0.clone();
        Box::pin(async move {
            let resolver = state
                .get_or_try_init(|| async { build_resolver(TokioResolver::builder_tokio()?) })
                .await?;
            let lookup = resolver.lookup_ip(name.as_str()).await?;
            let addresses: reqwest::dns::Addrs = Box::new(
                lookup
                    .iter()
                    .map(|ip| SocketAddr::new(ip, 0))
                    .collect::<Vec<_>>()
                    .into_iter(),
            );
            Ok(addresses)
        })
    }
}

fn build_resolver(
    mut builder: ResolverBuilder<TokioRuntimeProvider>,
) -> Result<TokioResolver, NetError> {
    // A silent family must leave room inside the existing HTTP request deadline.
    builder.options_mut().timeout = std::time::Duration::from_millis(250);
    builder.options_mut().attempts = 1;
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_resolver::proto::{
        op::Message,
        rr::{
            rdata::{A, AAAA},
            RData, Record, RecordType,
        },
    };
    use hickory_resolver::{
        config::{NameServerConfig, ResolverConfig},
        net::runtime::TokioRuntimeProvider,
    };
    use sentry::{
        transports::ReqwestHttpTransportOptions, Transport, TransportFactory, TransportOptions,
    };
    use std::{
        io::{Read, Write},
        net::{TcpListener, UdpSocket},
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant},
    };
    use web3_proxy::prelude::sentry;

    struct NativeTransport(reqwest::Client);
    impl TransportFactory for NativeTransport {
        fn create_transport_with_options(&self, options: TransportOptions) -> Arc<dyn Transport> {
            Arc::new(
                ReqwestHttpTransportOptions::from(options)
                    .with_client(self.0.clone())
                    .build(),
            )
        }
    }

    #[test]
    fn unanswered_dns_cannot_hold_native_sentry_shutdown_open() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let dns = UdpSocket::bind("127.0.0.1:0").unwrap();
        dns.set_read_timeout(Some(Duration::from_secs(4))).unwrap();
        let resolver = {
            let _entered = runtime.enter();
            let address = dns.local_addr().unwrap();
            let mut server = NameServerConfig::udp(address.ip());
            server.connections[0].port = address.port();
            let mut builder = TokioResolver::builder_with_config(
                ResolverConfig::from_name_servers(vec![server]),
                TokioRuntimeProvider::default(),
            );
            builder.options_mut().timeout = Duration::from_secs(30);
            builder.build().unwrap()
        };
        let http = reqwest::Client::builder()
            .no_proxy()
            .https_only(true)
            .tls_danger_accept_invalid_certs(false)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(2))
            .dns_resolver(Arc::new(SentryDns(Arc::new(OnceCell::new_with(Some(
                resolver,
            ))))))
            .build()
            .unwrap();
        let options = sentry::ClientOptions::from(
            "https://public@collector.fixture/1"
                .parse::<sentry::types::Dsn>()
                .unwrap(),
        )
        .transport(NativeTransport(http));
        let client = sentry::Client::from(options);
        client.capture_event(
            sentry::protocol::Event {
                level: sentry::Level::Error,
                ..Default::default()
            },
            None,
        );
        let mut query = [0; 2048];
        assert!(
            dns.recv_from(&mut query).unwrap().0 > 12,
            "Offline DNS request must be observed"
        );
        let started = Instant::now();
        client.close(Some(Duration::from_millis(20)));
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "Native SDK teardown must not wait for the30-second DNS response"
        );
    }

    fn resolved_families(only: Option<RecordType>) -> Vec<SocketAddr> {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let dns = UdpSocket::bind("127.0.0.1:0").unwrap();
        dns.set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        let address = dns.local_addr().unwrap();
        let observed = Arc::new(AtomicUsize::new(0));
        let seen = observed.clone();
        let (stop, stopped) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(4);
            let mut packet = [0; 2048];
            while Instant::now() < deadline && stopped.try_recv().is_err() {
                let (length, peer) = match dns.recv_from(&mut packet) {
                    Ok(packet) => packet,
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) =>
                    {
                        continue
                    }
                    Err(error) => panic!("Offline DNS collector failed: {error}"),
                };
                let request = Message::from_vec(&packet[..length]).unwrap();
                let query = request.queries[0].clone();
                let (flag, value) = match query.query_type() {
                    RecordType::A => (1, RData::A(A(std::net::Ipv4Addr::LOCALHOST))),
                    RecordType::AAAA => (2, RData::AAAA(AAAA(std::net::Ipv6Addr::LOCALHOST))),
                    kind => panic!("Unexpected offline DNS question: {kind:?}"),
                };
                seen.fetch_or(flag, Ordering::SeqCst);
                if only.is_some_and(|available| available != query.query_type()) {
                    continue;
                }
                let mut response = Message::response(request.metadata.id, request.metadata.op_code);
                response.metadata.authoritative = true;
                response.edns = request.edns.clone();
                response
                    .add_query(query.clone())
                    .add_answer(Record::from_rdata(query.name().clone(), 60, value));
                dns.send_to(&response.to_vec().unwrap(), peer).unwrap();
            }
        });
        let resolver = {
            let _entered = runtime.enter();
            let mut nameserver = NameServerConfig::udp(address.ip());
            nameserver.connections[0].port = address.port();
            build_resolver(TokioResolver::builder_with_config(
                ResolverConfig::from_name_servers(vec![nameserver]),
                TokioRuntimeProvider::default(),
            ))
            .unwrap()
        };
        let dns = SentryDns(Arc::new(OnceCell::new_with(Some(resolver))));
        let started = Instant::now();
        let result = runtime.block_on(async {
            tokio::time::timeout(
                Duration::from_millis(1500),
                reqwest::dns::Resolve::resolve(&dns, "families.fixture".parse().unwrap()),
            )
            .await
        });
        let _ = stop.send(());
        server.join().unwrap();
        assert_eq!(
            observed.load(Ordering::SeqCst),
            3,
            "Both DNS question families must be observed"
        );
        let addresses = result
            .expect("Usable DNS answers must fit inside the HTTP deadline")
            .unwrap()
            .collect::<Vec<_>>();
        if only.is_some() {
            assert!(
                started.elapsed() >= Duration::from_millis(200),
                "Missing family must remain a real unanswered lookup"
            );
        }
        addresses
    }

    #[test]
    fn sentry_resolver_keeps_both_a_and_aaaa_addresses() {
        let addresses = resolved_families(None);
        assert!(addresses.iter().any(SocketAddr::is_ipv4));
        assert!(addresses.iter().any(SocketAddr::is_ipv6));
    }

    #[test]
    fn sentry_resolver_uses_a_when_aaaa_is_silent() {
        let addresses = resolved_families(Some(RecordType::A));
        assert!(addresses.iter().any(SocketAddr::is_ipv4));
        assert!(addresses.iter().all(SocketAddr::is_ipv4));
    }

    #[test]
    fn sentry_resolver_uses_aaaa_when_a_is_silent() {
        let addresses = resolved_families(Some(RecordType::AAAA));
        assert!(addresses.iter().any(SocketAddr::is_ipv6));
        assert!(addresses.iter().all(SocketAddr::is_ipv6));
    }

    struct ApplicationDns(SocketAddr, Arc<AtomicUsize>);
    impl reqwest::dns::Resolve for ApplicationDns {
        fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
            self.1.fetch_add(1, Ordering::SeqCst);
            let address = self.0;
            Box::pin(async move { Ok(Box::new([address].into_iter()) as reqwest::dns::Addrs) })
        }
    }

    #[test]
    fn sentry_resolver_does_not_replace_an_ordinary_client_resolver() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + Duration::from_secs(4);
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => panic!("Offline application collector did not accept: {error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(4)))
                .unwrap();
            let mut request = [0; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nordinary",
                )
                .unwrap();
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let ordinary = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(4))
            .dns_resolver(Arc::new(ApplicationDns(address, calls.clone())))
            .build()
            .unwrap();
        let _sentry = reqwest::Client::builder()
            .dns_resolver(Arc::new(SentryDns::default()))
            .build()
            .unwrap();
        let body = runtime.block_on(async {
            ordinary
                .get(format!("http://application.fixture:{}/", address.port()))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        });
        server.join().unwrap();
        assert_eq!(body, "ordinary");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
