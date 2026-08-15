use crate::sub_commands::ProxydSubCommand;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::{thread, time::Duration};
use web3_proxy::config::{AppConfig, TopConfig, Web3RpcConfig};
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::hashbrown::HashMap;
use web3_proxy::prelude::serde_json::json;
use web3_proxy::prelude::tokio::{
    runtime::Builder,
    sync::broadcast::{self, error::SendError},
    time::{sleep, Instant},
};
use web3_proxy::prelude::url::Url;
use web3_proxy::rpcs::provider::{connect_http, AlloyHttpProvider};
use web3_proxy::test_utils::TestAnvil;

pub struct TestApp {
    pub proxy_handle: Option<thread::JoinHandle<anyhow::Result<()>>>,
    pub proxy_provider: AlloyHttpProvider,
    pub proxy_url: Url,
    shutdown_sender: broadcast::Sender<()>,
}

impl TestApp {
    pub async fn spawn(anvil: &TestAnvil) -> Self {
        let app_config: AppConfig = serde_json::from_value(json!({
            "chain_id": anvil.instance.chain_id(),
            "min_sum_soft_limit": 1,
            "min_synced_rpcs": 1,
        }))
        .unwrap();

        let top_config = TopConfig {
            app: app_config,
            balanced_rpcs: HashMap::from([(
                "anvil".to_string(),
                Web3RpcConfig {
                    http_url: Some(anvil.instance.endpoint()),
                    ws_url: Some(anvil.instance.ws_endpoint()),
                    ..Default::default()
                },
            )]),
            private_rpcs: HashMap::from([(
                "anvil_private".to_string(),
                Web3RpcConfig {
                    http_url: Some(anvil.instance.endpoint()),
                    ws_url: Some(anvil.instance.ws_endpoint()),
                    ..Default::default()
                },
            )]),
            bundler_4337_rpcs: Default::default(),
            extra: Default::default(),
        };

        let (shutdown_sender, _) = broadcast::channel(1);
        let frontend_port = Arc::new(AtomicU16::new(0));
        let prometheus_port = Arc::new(AtomicU16::new(0));

        let proxy_handle = {
            let frontend_port = frontend_port.clone();
            let shutdown_sender = shutdown_sender.clone();
            thread::spawn(move || {
                Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(4)
                    .build()
                    .unwrap()
                    .block_on(ProxydSubCommand::_main(
                        top_config,
                        None,
                        frontend_port,
                        prometheus_port,
                        shutdown_sender,
                    ))
            })
        };

        let start = Instant::now();
        while frontend_port.load(Ordering::SeqCst) == 0 {
            assert!(
                start.elapsed() <= Duration::from_secs(30),
                "proxy took too long to start"
            );
            sleep(Duration::from_millis(10)).await;
        }

        let proxy_url: Url = format!("http://127.0.0.1:{}", frontend_port.load(Ordering::SeqCst))
            .parse()
            .unwrap();
        let proxy_provider = connect_http(proxy_url.clone()).unwrap();

        Self {
            proxy_handle: Some(proxy_handle),
            proxy_provider,
            proxy_url,
            shutdown_sender,
        }
    }

    pub fn stop(&self) -> Result<usize, SendError<()>> {
        self.shutdown_sender.send(())
    }

    pub fn wait_for_stop(mut self) {
        let _ = self.stop();
        if let Some(handle) = self.proxy_handle.take() {
            handle.join().unwrap().unwrap();
        }
    }
}

impl Drop for TestApp {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
