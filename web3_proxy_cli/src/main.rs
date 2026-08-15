use web3_proxy::prelude::*;

use anyhow::Context;
use argh::FromArgs;
use ethers::types::U256;
use pagerduty_rs::eventsv2async::EventsV2 as PagerdutyAsyncEventsV2;
use pagerduty_rs::eventsv2sync::EventsV2 as PagerdutySyncEventsV2;
use sentry::types::Dsn;
use std::{
    borrow::Cow,
    fs, panic,
    path::Path,
    sync::atomic::{self, AtomicUsize},
};
use tokio::runtime;
use tracing::{info, warn};
use tracing_subscriber::{prelude::*, EnvFilter};
use web3_proxy::pagerduty::panic_handler;
use web3_proxy::{app::APP_USER_AGENT, config::TopConfig};
use web3_proxy_cli::sub_commands;

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "deadlock_detection")]
use {parking_lot::deadlock, std::thread, tokio::time::Duration};

#[derive(Debug, FromArgs)]
/// Command line interface for admins to interact with web3_proxy
pub struct Web3ProxyCli {
    /// path to the application config (only required for some commands; defaults to dev config).
    #[argh(option)]
    pub config: Option<String>,

    /// number of worker threads. Defaults to the number of logical processors
    #[argh(option, default = "0")]
    pub workers: usize,

    /// if no config, what sentry url should the client should connect to (only required for some commands)
    #[argh(option)]
    pub sentry_url: Option<Dsn>,

    /// this one cli can do multiple things
    #[argh(subcommand)]
    sub_command: SubCommand,
}

#[derive(FromArgs, PartialEq, Debug)]
#[argh(subcommand)]
enum SubCommand {
    CheckConfig(sub_commands::CheckConfigSubCommand),
    Pagerduty(sub_commands::PagerdutySubCommand),
    PopularityContest(sub_commands::PopularityContestSubCommand),
    Proxyd(sub_commands::ProxydSubCommand),
    Sentryd(sub_commands::SentrydSubCommand),
}

fn main() -> anyhow::Result<()> {
    // this probably won't matter for us in docker, but better safe than sorry
    fdlimit::raise_fd_limit()?;

    #[cfg(feature = "deadlock_detection")]
    {
        // spawn a thread for deadlock detection
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(10));
            let deadlocks = deadlock::check_deadlock();
            if deadlocks.is_empty() {
                continue;
            }

            let mut msg = format!("{} deadlocks detected\n", deadlocks.len());

            for (i, threads) in deadlocks.iter().enumerate() {
                msg += &format!("Deadlock #{}", i);
                for t in threads {
                    msg += &format!("Thread Id {:#?}\n", t.thread_id());
                    msg += &format!("{:#?}\n", t.backtrace());
                }
            }

            panic!("{:#}", msg);
        });
    }

    // if RUST_LOG isn't set, configure a default
    let mut rust_log = match std::env::var("RUST_LOG") {
        Ok(x) => x,
        Err(_) => match std::env::var("WEB3_PROXY_TRACE").map(|x| x == "true") {
            Ok(true) => {
                vec![
                    "info",
                    "ethers=debug",
                    "ethers_providers::rpc=off",
                    "ethers_providers=debug",
                    "web3_proxy::rpcs::blockchain=info",
                    "web3_proxy::rpcs::request=debug",
                    "web3_proxy=trace",
                    "web3_proxy_cli=trace",
                ]
            }
            _ => {
                vec![
                    "info",
                    "ethers=debug",
                    "ethers_providers::rpc=off",
                    "ethers_providers=error",
                    "web3_proxy::rpcs::consensus=info",
                    "web3_proxy=debug",
                    "web3_proxy_cli=debug",
                ]
            }
        }
        .join(","),
    };

    if let Ok(extra_rust_log) = std::env::var("EXTRA_RUST_LOG") {
        rust_log.push(',');
        rust_log.push_str(&extra_rust_log);
    }

    let mut cli_config: Web3ProxyCli = argh::from_env();

    if cli_config.config.is_none() && cli_config.sentry_url.is_none() {
        // TODO: default to example.toml if development.toml doesn't exist
        info!("defaulting to development config");
        cli_config.config = Some("./config/development.toml".to_string());
    }

    let (top_config, top_config_path) = if let Some(top_config_path) = cli_config.config.clone() {
        let top_config_path = Path::new(&top_config_path)
            .canonicalize()
            .context(format!("checking for config at {}", top_config_path))?;

        let top_config: String = fs::read_to_string(top_config_path.clone())?;

        let mut top_config: TopConfig = toml::from_str(&top_config)?;

        if let Some(sentry_url) = top_config.app.sentry_url.clone() {
            cli_config.sentry_url = Some(sentry_url);
        }

        if top_config.app.chain_id == 137 {
            // TODO: these numbers are arbitrary. i think the maticnetwork/erigon fork has a bug
            if top_config.app.gas_increase_min.is_none() {
                top_config.app.gas_increase_min = Some(U256::from(40_000));
            }

            if top_config.app.gas_increase_percent.is_none() {
                top_config.app.gas_increase_percent = Some(U256::from(40));
            }
        }

        top_config.clean();

        (Some(top_config), Some(top_config_path))
    } else {
        (None, None)
    };

    // TODO: put sentry behind a feature
    let sentry_env = std::env::var("SENTRY_ENV")
        .map(Cow::from)
        .unwrap_or("production".into());

    // set up sentry connection
    // this guard does nothing is sentry_url is None
    let mut sentry_options = sentry::ClientOptions::new()
        .maybe_release(sentry::release_name!())
        .environment(sentry_env)
        // TODO: make sample_rate configurable!
        .sample_rate(1.0)
        // TODO: make traces_sample_rate configurable! (its not yet available for our rust project)
        .traces_sample_rate(0.0);
    sentry_options.dsn = cli_config.sentry_url.clone();
    let _sentry_guard = sentry::init(sentry_options);

    sentry::configure_scope(|scope| {
        let chain_id = top_config.as_ref().map(|x| x.app.chain_id).unwrap_or(0);
        scope.set_tag("chain_id", chain_id);

        if let Ok(llama_env) = std::env::var("LLAMA_ENV") {
            scope.set_tag("llama_env", llama_env);
        }
    });

    let env_filter = EnvFilter::builder().parse(&rust_log)?;
    let fmt_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_filter(env_filter);

    let env_filter = EnvFilter::builder().parse(rust_log)?;
    let sentry_layer = sentry_tracing::layer().with_filter(env_filter);

    // build a `Subscriber` by combining layers
    let tracing_registry = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(sentry_layer);

    #[cfg(feature = "tokio-console")]
    let tracing_registry = {
        // TODO: i'm not sure if this env_filter is needed, but it seems like a good idea
        let env_filter = EnvFilter::builder().parse("tokio=trace,runtime=trace")?;
        let console_layer = console_subscriber::spawn().with_filter(env_filter);

        tracing_registry.with(console_layer)
    };

    tracing_registry.init();

    info!(%APP_USER_AGENT);

    // optionally connect to pagerduty
    // TODO: fix this nested result
    // TODO: get this out of the config file instead of the environment
    let (pagerduty_async, pagerduty_sync) = if let Ok(pagerduty_key) =
        std::env::var("PAGERDUTY_INTEGRATION_KEY")
    {
        let pagerduty_async =
            PagerdutyAsyncEventsV2::new(pagerduty_key.clone(), Some(APP_USER_AGENT.to_string()))?;
        let pagerduty_sync =
            PagerdutySyncEventsV2::new(pagerduty_key, Some(APP_USER_AGENT.to_string()))?;

        (Some(pagerduty_async), Some(pagerduty_sync))
    } else {
        info!("No PAGERDUTY_INTEGRATION_KEY");

        (None, None)
    };

    // panic handler that sends to pagerduty.
    // TODO: use the sentry handler if no pager duty. use default if no sentry
    if let Some(pagerduty_sync) = pagerduty_sync {
        let top_config = top_config.clone();

        panic::set_hook(Box::new(move |x| {
            panic_handler(top_config.clone(), &pagerduty_sync, x);
        }));
    }

    // set up tokio's async runtime
    let mut rt_builder = runtime::Builder::new_multi_thread();

    rt_builder.enable_io();
    rt_builder.enable_time();

    // TODO: debug option to enable and expose this
    // rt_builder.enable_metrics_poll_count_histogram();

    if cli_config.workers > 0 {
        rt_builder.worker_threads(cli_config.workers);
    }

    if let Some(ref top_config) = top_config {
        let chain_id = top_config.app.chain_id;

        rt_builder.thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            // TODO: what ordering? i think we want seqcst so that these all happen in order, but that might be stricter than we really need
            let worker_id = ATOMIC_ID.fetch_add(1, atomic::Ordering::SeqCst);
            // TODO: i think these max at 15 characters
            format!("web3-{}-{}", chain_id, worker_id)
        });
    }

    // start tokio's async runtime
    let rt = rt_builder.build()?;

    let num_workers = rt.metrics().num_workers();
    info!("num_workers: {}", num_workers);

    rt.block_on(async {
        match cli_config.sub_command {
            SubCommand::CheckConfig(command) => command.main().await,
            SubCommand::Pagerduty(command) => {
                if cli_config.sentry_url.is_none() {
                    warn!("sentry_url is not set; logs will only appear in this console");
                }
                command.main(pagerduty_async, top_config).await
            }
            SubCommand::PopularityContest(command) => command.main().await,
            SubCommand::Proxyd(command) => {
                let top_config = top_config.expect("--config is required to run proxyd");
                let top_config_path =
                    top_config_path.expect("path must be set when the config exists");
                command.main(top_config, top_config_path).await
            }
            SubCommand::Sentryd(command) => {
                if cli_config.sentry_url.is_none() {
                    warn!("sentry_url is not set; logs will only appear in this console");
                }
                command.main(pagerduty_async, top_config).await
            }
        }
    })
}
