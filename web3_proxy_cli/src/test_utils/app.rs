use crate::sub_commands::ProxydSubCommand;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::{thread, time::Duration};
use web3_proxy::config::{AppConfig, TopConfig, Web3RpcConfig};
use web3_proxy::prelude::alloy::providers::ProviderBuilder;
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::hashbrown::HashMap;
use web3_proxy::prelude::reqwest;
use web3_proxy::prelude::sonic_rs::{self, json};
use web3_proxy::prelude::tokio::{
    runtime::Builder,
    sync::broadcast::{self, error::SendError},
    sync::watch,
    time::{sleep, timeout_at, Instant},
};
use web3_proxy::prelude::url::Url;
use web3_proxy::rpcs::blockchain::BlockHeader;
use web3_proxy::rpcs::provider::AlloyHttpProvider;
use web3_proxy::test_utils::TestAnvil;

pub struct TestApp {
    pub proxy_handle: Option<thread::JoinHandle<anyhow::Result<()>>>,
    pub proxy_provider: AlloyHttpProvider,
    pub proxy_url: Url,
    head_block_receiver: watch::Receiver<Option<BlockHeader>>,
    shutdown_sender: broadcast::Sender<()>,
}

impl TestApp {
    pub async fn spawn(anvil: &TestAnvil) -> Self {
        let app_config: AppConfig = sonic_rs::from_value(&json!({
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
        let (watch_consensus_head_sender, head_block_receiver) = watch::channel(None);
        let frontend_port = Arc::new(AtomicU16::new(0));

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
                        shutdown_sender,
                        watch_consensus_head_sender,
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
        let client = reqwest::Client::new();
        let proxy_provider = ProviderBuilder::default().connect_reqwest(client, proxy_url.clone());

        Self {
            proxy_handle: Some(proxy_handle),
            proxy_provider,
            proxy_url,
            head_block_receiver,
            shutdown_sender,
        }
    }

    pub async fn wait_for_block(&self, target: u64) {
        let mut head_block_receiver = self.head_block_receiver.clone();
        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            let last_observed = head_block_receiver
                .borrow_and_update()
                .as_ref()
                .map(|block| block.number().to::<u64>());

            if last_observed.is_some_and(|block| block >= target) {
                return;
            }

            match timeout_at(deadline, head_block_receiver.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => panic!(
                    "consensus head channel closed while waiting for block {target}; last observed block: {last_observed:?}"
                ),
                Err(_) => panic!(
                    "timed out while waiting for consensus head block {target}; last observed block: {last_observed:?}"
                ),
            }
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
