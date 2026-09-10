use anyhow::{ensure, Context};
use argh::FromArgs;
use std::{path::PathBuf, time::Duration};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::broadcast,
};
use web3_proxy::{block_relay::BlockRelay, config::TopConfig, prelude::*};

/// Run only the block relay. Do not start the RPC proxy or any Ethereum clients.
#[derive(FromArgs, PartialEq, Debug, Eq)]
#[argh(subcommand, name = "block_relay")]
pub struct BlockRelaySubCommand {}

impl BlockRelaySubCommand {
    pub async fn main(self, config: TopConfig, path: PathBuf) -> anyhow::Result<()> {
        ensure!(
            config.block_relay.is_some(),
            "config requires [block_relay]"
        );
        let relay = BlockRelay::new();
        relay.apply(config.block_relay.as_ref()).await?;
        let mut terminate = signal(SignalKind::terminate())?;
        let (shutdown, _) = broadcast::channel(1);
        let mut run = tokio::spawn(relay.clone().run(config.app.chain_id, shutdown.subscribe()));
        let mut reload = tokio::time::interval(Duration::from_secs(1));
        let mut report = tokio::time::interval(Duration::from_secs(30));
        // File contents can contain credentials. Never include them or parse errors in logs.
        let mut contents = String::new();
        let result = loop {
            tokio::select! {
                result = &mut run => return result.context("relay task failed"),
                result = tokio::signal::ctrl_c() => break result.map_err(Into::into),
                _ = terminate.recv() => break Ok(()),
                _ = report.tick() => tracing::info!(status = %relay.snapshot(), "block relay status"),
                _ = reload.tick() => {
                    let path = path.clone();
                    let read = tokio::task::spawn_blocking(move || std::fs::read_to_string(path)).await;
                    let next = match read {
                        Ok(Ok(next)) => next,
                        _ => { tracing::error!("cannot read relay config; keeping current config"); continue; }
                    };
                    if next == contents { continue; }
                    let next_config = match TopConfig::from_toml_str(&next) {
                        Ok(next) if next.app.chain_id == config.app.chain_id => next,
                        _ => { tracing::error!("invalid relay config or changed chain ID; keeping current config"); continue; }
                    };
                    match relay.apply(next_config.block_relay.as_ref()).await {
                        Ok(()) => contents = next,
                        Err(error) => tracing::error!(%error, "cannot apply relay config; keeping current config"),
                    }
                }
            }
        };
        let _ = shutdown.send(());
        run.await.context("relay task failed")?;
        result
    }
}
