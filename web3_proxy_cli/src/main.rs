use web3_proxy::prelude::*;

use anyhow::Context;
use argh::FromArgs;
use sentry::types::Dsn;
use std::{
    borrow::Cow,
    fs,
    path::{absolute, PathBuf},
    sync::atomic::{self, AtomicUsize},
};
use tokio::runtime;
use tracing::{info, warn};
use tracing_subscriber::{filter::FilterExt, prelude::*, EnvFilter};
use web3_proxy::prelude::alloy::primitives::U256;
use web3_proxy::{app::APP_USER_AGENT, config::TopConfig};
use web3_proxy_cli::{sub_commands, DEFAULT_CONFIG_PATH};

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Keep timing diagnostics separate from ordinary application debug logs.
const DEFAULT_LOG_DIRECTIVES: &[&str] = &[
    "info",
    "alloy_provider=error",
    "alloy_transport=error",
    "web3_proxy::rpcs::consensus=info",
    "web3_proxy=debug",
    "web3_proxy_cli=debug",
    "web3_proxy::perf=off",
    "latency::perf=off",
];

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
    CompareRpcs(sub_commands::CompareRpcsSubCommand),
    BlockRelay(sub_commands::BlockRelaySubCommand),
    CheckConfig(sub_commands::CheckConfigSubCommand),
    PopularityContest(sub_commands::PopularityContestSubCommand),
    Proxyd(sub_commands::ProxydSubCommand),
    Sentryd(sub_commands::SentrydSubCommand),
}

fn config_path(path: &str) -> anyhow::Result<PathBuf> {
    // Keep the logical filename: kubelet replaces its symlink target on reload.
    absolute(path).with_context(|| format!("making config path absolute: {path}"))
}

fn optional_sentry_layer<S>(enabled: bool, filter: EnvFilter) -> impl tracing_subscriber::Layer<S>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    // An unfiltered None layer can keep tracing::enabled! true even when
    // every output rejects the event. Filter the optional layer too.
    // This client reports errors and breadcrumbs, not transactions. Filtering
    // spans inside SentryLayer is too late: its enter/exit callbacks still run
    // for spans that another layer accepts. Reject spans at the layer boundary
    // so an HTTP request never clones a Sentry hub or locks its scope per poll.
    let events = tracing_subscriber::filter::filter_fn(move |metadata| {
        enabled && metadata.is_event() && *metadata.level() <= tracing::Level::INFO
    });
    enabled
        .then(sentry_tracing::layer)
        .with_filter(filter.and(events))
}

fn main() -> anyhow::Result<()> {
    load_dotenv()?;

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
                    "alloy_provider=debug",
                    "alloy_transport=debug",
                    "web3_proxy::rpcs::blockchain=info",
                    "web3_proxy::rpcs::request=debug",
                    "web3_proxy=trace",
                    "web3_proxy_cli=trace",
                ]
            }
            _ => DEFAULT_LOG_DIRECTIVES.to_vec(),
        }
        .join(","),
    };

    if let Ok(extra_rust_log) = std::env::var("EXTRA_RUST_LOG") {
        rust_log.push(',');
        rust_log.push_str(&extra_rust_log);
    }

    let mut cli_config: Web3ProxyCli = argh::from_env();

    // The observer owns its endpoint config and exit codes. It must not load a
    // server config, initialize Sentry, or start any proxy services.
    if let SubCommand::CompareRpcs(command) = cli_config.sub_command {
        let rt = runtime::Builder::new_multi_thread()
            .worker_threads(cli_config.workers.max(1))
            .enable_all()
            .build()?;
        let code = rt.block_on(command.main());
        rt.shutdown_timeout(std::time::Duration::from_secs(2));
        std::process::exit(i32::from(code));
    }

    if cli_config.config.is_none() && cli_config.sentry_url.is_none() {
        // TODO: default to example.toml if development.toml doesn't exist
        info!("defaulting to development config");
        cli_config.config = Some(DEFAULT_CONFIG_PATH.to_string());
    }

    let (top_config, top_config_path) = if let Some(top_config_path) = cli_config.config.clone() {
        let top_config_path = config_path(&top_config_path)?;

        let top_config: String = fs::read_to_string(top_config_path.clone())?;

        let mut top_config = TopConfig::from_toml_str(&top_config)?;

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
    // A disabled client cannot collect events. Avoid the layer's per-span hub
    // and scope locks when Sentry is not configured.
    let sentry_layer = optional_sentry_layer(_sentry_guard.is_enabled(), env_filter);

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

    info!(
        sentry_enabled = _sentry_guard.is_enabled(),
        "Sentry error reporting configured"
    );
    info!(%APP_USER_AGENT);

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

    let result = rt.block_on(async {
        match cli_config.sub_command {
            SubCommand::CompareRpcs(_) => unreachable!("observer handled before server setup"),
            SubCommand::BlockRelay(command) => {
                command
                    .main(
                        top_config.context("--config is required to run block_relay")?,
                        top_config_path.context("config path is required")?,
                    )
                    .await
            }
            SubCommand::CheckConfig(command) => command.main().await,
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
                command.main().await
            }
        }
    });
    // Blocking disk or crypto work must not hold container shutdown indefinitely.
    rt.shutdown_timeout(std::time::Duration::from_secs(2));
    let _ = web3_proxy::flush_diagnostics(std::time::Duration::from_millis(100));
    result
}

fn load_dotenv() -> anyhow::Result<()> {
    match dotenvy::dotenv() {
        Ok(_) => Ok(()),
        Err(error) if error.not_found() => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::{config_path, sentry, DEFAULT_LOG_DIRECTIVES};
    use std::{fs, os::unix::fs::symlink};

    // Callsite interest is process-wide. Keep the temporary dispatchers from
    // changing each other's cached interests while asserting disabled sites.
    static TRACING_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn default_debug_logging_keeps_timing_targets_opt_in() {
        let _guard = TRACING_TEST_LOCK.lock().unwrap();
        use tracing_subscriber::{prelude::*, EnvFilter};
        let defaults = DEFAULT_LOG_DIRECTIVES.join(",");
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::new(&defaults)));
        tracing::subscriber::with_default(subscriber, || {
            assert!(tracing::enabled!(target: "web3_proxy", tracing::Level::DEBUG));
            assert!(!tracing::enabled!(target: "web3_proxy::perf", tracing::Level::DEBUG));
            assert!(!tracing::enabled!(target: "latency::perf", tracing::Level::DEBUG));
        });
        let explicit = format!("{defaults},web3_proxy::perf=debug,latency::perf=debug");
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_filter(EnvFilter::new(explicit)));
        tracing::subscriber::with_default(subscriber, || {
            assert!(tracing::enabled!(target: "web3_proxy::perf", tracing::Level::DEBUG));
            assert!(tracing::enabled!(target: "latency::perf", tracing::Level::DEBUG));
        });
    }

    #[test]
    fn optional_sentry_respects_timing_filters_before_sampling() {
        let _guard = TRACING_TEST_LOCK.lock().unwrap();
        use tracing_subscriber::{prelude::*, EnvFilter};
        for sentry_enabled in [false, true] {
            for (directives, timing_enabled) in [
                (DEFAULT_LOG_DIRECTIVES.join(","), false),
                ("info".to_owned(), false),
                (
                    "info,web3_proxy::perf=debug,latency::perf=debug".to_owned(),
                    true,
                ),
            ] {
                let fmt = tracing_subscriber::fmt::layer().with_filter(EnvFilter::new(&directives));
                let sentry =
                    super::optional_sentry_layer(sentry_enabled, EnvFilter::new(&directives));
                let subscriber = tracing_subscriber::registry().with(fmt).with(sentry);
                tracing::subscriber::with_default(subscriber, || {
                    assert_eq!(
                        tracing::enabled!(target: "web3_proxy::perf", tracing::Level::DEBUG),
                        timing_enabled
                    );
                    assert_eq!(
                        tracing::enabled!(target: "latency::perf", tracing::Level::DEBUG),
                        timing_enabled
                    );
                });
            }
        }
    }

    #[test]
    fn sentry_reports_errors_without_instrumenting_request_spans() {
        let _guard = TRACING_TEST_LOCK.lock().unwrap();
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::{prelude::*, EnvFilter};

        #[derive(Default)]
        struct Capture(Mutex<Vec<sentry::Envelope>>);
        impl sentry::Transport for Capture {
            fn send_envelope(&self, envelope: sentry::Envelope) {
                self.0.lock().unwrap().push(envelope);
            }
        }

        let transport = Arc::new(Capture::default());
        let transactions = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sampled = transactions.clone();
        let client = Arc::new(sentry::Client::from(
            sentry::ClientOptions::new()
                .dsn("https://public@sentry.invalid/1")
                .transport(transport.clone())
                .traces_sampler(move |_| {
                    sampled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    0.0
                }),
        ));
        assert!(client.is_enabled());
        let hub = Arc::new(sentry::Hub::new(Some(client), Arc::default()));
        let fmt = tracing_subscriber::fmt::layer()
            .with_writer(std::io::sink)
            .with_filter(EnvFilter::new("trace"));
        let sentry = super::optional_sentry_layer(true, EnvFilter::new("trace"));
        let subscriber = tracing_subscriber::registry().with(fmt).with(sentry);
        sentry::Hub::run(hub.clone(), || {
            tracing::subscriber::with_default(subscriber, || {
                // The HTTP middleware uses this fallback at ordinary log levels.
                let request = tracing::error_span!("request", id = "synthetic-request");
                for _ in 0..3 {
                    let _entered = request.enter();
                    assert!(Arc::ptr_eq(&sentry::Hub::current(), &hub));
                    sentry::configure_scope(|scope| assert!(scope.get_span().is_none()));
                    tracing::trace!("ignored trace");
                    tracing::debug!("ignored debug");
                }
                tracing::info!("synthetic breadcrumb");
                tracing::warn!("synthetic warning");
                tracing::error!(diagnostic = true, "synthetic failure");
            });
        });
        let envelopes = transport.0.lock().unwrap();
        assert_eq!(envelopes.len(), 1);
        let event = envelopes[0].event().unwrap();
        assert_eq!(event.message.as_deref(), Some("synthetic failure"));
        assert_eq!(event.breadcrumbs.len(), 2);
        let sentry::protocol::Context::Other(fields) = &event.contexts["Rust Tracing Fields"]
        else {
            panic!("missing structured tracing fields");
        };
        assert_eq!(fields.get("diagnostic"), Some(&serde_json::json!(true)));
        assert_eq!(transactions.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(event.transaction.is_none());
    }

    #[test]
    fn config_reload_follows_rotated_projected_volume() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let first = root.join("generation-1");
        let second = root.join("generation-2");
        fs::create_dir(&first).unwrap();
        fs::create_dir(&second).unwrap();
        fs::write(first.join("relay.toml"), "mode = \"inject\"\n").unwrap();
        fs::write(second.join("relay.toml"), "mode = \"observe\"\n").unwrap();
        symlink("generation-1", root.join("..data")).unwrap();
        symlink("..data/relay.toml", root.join("relay.toml")).unwrap();
        let path = config_path(root.join("relay.toml").to_str().unwrap()).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "mode = \"inject\"\n");

        // Match kubelet's atomic ..data replacement and old-generation cleanup.
        symlink("generation-2", root.join("..data-next")).unwrap();
        fs::rename(root.join("..data-next"), root.join("..data")).unwrap();
        fs::remove_dir_all(first).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "mode = \"observe\"\n");
    }
}
