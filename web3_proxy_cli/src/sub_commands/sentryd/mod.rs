mod compare;
mod simple;

use std::time::Duration;
use tracing::{debug, error, info, warn, Level};
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::argh::{self, FromArgs};
use web3_proxy::prelude::futures::{
    stream::{FuturesUnordered, StreamExt},
    Future,
};
use web3_proxy::prelude::tokio;
use web3_proxy::prelude::tokio::sync::mpsc;
use web3_proxy::prelude::tokio::time::{interval, MissedTickBehavior};

#[derive(FromArgs, PartialEq, Debug, Eq)]
/// Loop health checks and log failures
#[argh(subcommand, name = "sentryd")]
pub struct SentrydSubCommand {
    #[argh(positional)]
    /// the main (HTTP only) web3-proxy being checked.
    web3_proxy: String,

    #[argh(option)]
    /// warning threshold for age of the best known head block
    max_age: i64,

    #[argh(option)]
    /// warning threshold for seconds between the rpc and best other_rpc's head blocks
    max_lag: i64,

    #[argh(option)]
    /// other (HTTP only) rpcs to compare the main rpc to
    other_rpc: Vec<String>,

    #[argh(option)]
    /// other (HTTP only) web3-proxies to compare the main rpc to
    other_proxy: Vec<String>,

    #[argh(option)]
    /// how many seconds between running checks
    seconds: Option<u64>,
}

#[derive(Debug)]
pub struct SentrydError {
    /// The class/type of the event, for example ping failure or cpu load
    class: String,
    /// Log level for the failed check
    level: Level,
    /// The check error and its context chain
    error: anyhow::Error,
}

/// helper for creating SentrydErrors
#[derive(Clone)]
pub struct SentrydErrorBuilder {
    class: String,
    level: Level,
}

impl SentrydErrorBuilder {
    fn build(&self, err: anyhow::Error) -> SentrydError {
        SentrydError {
            class: self.class.to_owned(),
            level: self.level,
            error: err,
        }
    }

    fn result(&self, err: anyhow::Error) -> SentrydResult {
        Err(self.build(err))
    }
}

type SentrydResult = Result<(), SentrydError>;

impl SentrydSubCommand {
    pub async fn main(self) -> anyhow::Result<()> {
        // sentry logging should already be configured

        let primary_proxy = self.web3_proxy.trim_end_matches('/').to_string();

        let other_proxy: Vec<_> = self
            .other_proxy
            .into_iter()
            .map(|x| x.trim_end_matches('/').to_string())
            .collect();

        let other_rpc: Vec<_> = self
            .other_rpc
            .into_iter()
            .map(|x| x.trim_end_matches('/').to_string())
            .collect();

        let seconds = self.seconds.unwrap_or(60);

        let mut handles = FuturesUnordered::new();

        // Channel and task for logging check failures.
        let (error_sender, mut error_receiver) = mpsc::channel::<SentrydError>(10);

        {
            let error_handler_f = async move {
                while let Some(err) = error_receiver.recv().await {
                    if err.level == Level::ERROR {
                        error!(class = %err.class, error = ?err.error, "check failed");
                    } else if err.level == Level::WARN {
                        warn!(class = %err.class, error = ?err.error, "check failed");
                    } else if err.level == Level::INFO {
                        info!(class = %err.class, error = ?err.error, "check failed");
                    } else {
                        debug!(class = %err.class, error = ?err.error, "check failed");
                    }
                }

                Ok(())
            };

            handles.push(tokio::spawn(error_handler_f));
        }

        // spawn a bunch of health check loops that do their checks on an interval

        // check the main rpc's /health endpoint
        {
            let url = format!("{}/health", primary_proxy);
            let error_sender = error_sender.clone();

            // TODO: what timeout?
            let timeout = Duration::from_secs(5);

            let loop_f = a_loop(
                "main /health",
                seconds,
                Level::ERROR,
                error_sender,
                move |error_builder| simple::main(error_builder, url.clone(), timeout),
            );

            handles.push(tokio::spawn(loop_f));
        }
        // check any other web3-proxy /health endpoints
        for other_web3_proxy in other_proxy.iter() {
            let url = format!("{}/health", other_web3_proxy);

            let error_sender = error_sender.clone();

            // TODO: what timeout?
            let timeout = Duration::from_secs(5);

            let loop_f = a_loop(
                "other /health",
                seconds,
                Level::WARN,
                error_sender,
                move |error_builder| simple::main(error_builder, url.clone(), timeout),
            );

            handles.push(tokio::spawn(loop_f));
        }

        // compare the main web3-proxy head block to all web3-proxies and rpcs
        {
            let max_age = self.max_age;
            let max_lag = self.max_lag;
            let primary_proxy = primary_proxy.clone();
            let error_sender = error_sender.clone();

            let mut others = other_proxy.clone();

            others.extend(other_rpc);

            let loop_f = a_loop(
                "head block comparison",
                seconds,
                Level::ERROR,
                error_sender,
                move |error_builder| {
                    compare::main(
                        error_builder,
                        primary_proxy.clone(),
                        others.clone(),
                        max_age,
                        max_lag,
                    )
                },
            );

            handles.push(tokio::spawn(loop_f));
        }

        // wait for any returned values (if everything is working, they will all run forever)
        while let Some(x) = handles.next().await {
            // any errors that make it here will end the program
            x??;
        }

        Ok(())
    }
}

async fn a_loop<T>(
    class: &str,
    seconds: u64,
    error_level: Level,
    error_sender: mpsc::Sender<SentrydError>,
    f: impl Fn(SentrydErrorBuilder) -> T,
) -> anyhow::Result<()>
where
    T: Future<Output = SentrydResult> + Send + 'static,
{
    let error_builder = SentrydErrorBuilder {
        class: class.to_owned(),
        level: error_level,
    };

    let mut interval = interval(Duration::from_secs(seconds));

    // TODO: should we warn if there are delays?
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        if let Err(err) = f(error_builder.clone()).await {
            error_sender.send(err).await?;
        };
    }
}
