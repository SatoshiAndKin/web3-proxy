use prettytable::{row, Table};
use std::cmp::Reverse;
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::argh::{self, FromArgs};
use web3_proxy::prelude::ordered_float::OrderedFloat;
use web3_proxy::prelude::reqwest;
use web3_proxy::prelude::sonic_rs::{self, JsonContainerTrait, JsonValueTrait, Object, Value};

#[derive(FromArgs, PartialEq, Debug)]
/// show what nodes are used most often
#[argh(subcommand, name = "popularity_contest")]
pub struct PopularityContestSubCommand {
    #[argh(positional)]
    /// the web3-proxy url
    /// TODO: query multiple and add them together
    rpc: String,
}

#[derive(Debug)]
struct BackendRpcData<'a> {
    active_requests: u64,
    backup: bool,
    block_data_limit: u64,
    head_block: u64,
    head_delay_ms: f64,
    median_latency_ms: f64,
    name: &'a str,
    peak_latency_ms: f64,
    tier: u64,
    total_requests: u64,
    weighted_latency_ms: f64,
}

/// TODO: i do not understand why we have this. we should be using alloy types
fn head_block_number(conn: &Object) -> u64 {
    conn.get(&"head_block")
        .and_then(|x| x.get("block"))
        .and_then(|x| x.get("number"))
        .and_then(|x| x.as_u64())
        .unwrap_or_default()
}

impl PopularityContestSubCommand {
    pub async fn main(self) -> anyhow::Result<()> {
        let response = reqwest::get(format!("{}/status", self.rpc)).await?;
        let body = response.bytes().await?;
        let x: Value = sonic_rs::from_slice(&body)?;

        let conns = x
            .as_object()
            .unwrap()
            .get(&"balanced_rpcs")
            .unwrap()
            .as_object()
            .unwrap()
            .get(&"conns")
            .unwrap()
            .as_array()
            .unwrap();

        let mut highest_block = 0;
        let mut rpc_data = vec![];
        let mut all_requests = 0;

        for conn in conns {
            let conn = conn.as_object().unwrap();

            let name = conn
                .get(&"display_name")
                .unwrap_or_else(|| conn.get(&"name").unwrap())
                .as_str()
                .unwrap_or("unknown");

            let tier = conn.get(&"tier").unwrap().as_u64().unwrap();

            let backup = conn.get(&"backup").unwrap().as_bool().unwrap();

            let block_data_limit = conn
                .get(&"block_data_limit")
                .and_then(|x| x.as_u64())
                .unwrap_or(u64::MAX);

            let total_requests = conn
                .get(&"total_requests")
                .and_then(|x| x.as_u64())
                .unwrap_or_default();

            let active_requests = conn
                .get(&"active_requests")
                .and_then(|x| x.as_u64())
                .unwrap_or_default();

            let head_block = head_block_number(conn);

            highest_block = highest_block.max(head_block);

            // TODO: this was moved to an async lock and so serialize can't fetch it
            let head_delay_ms = conn
                .get(&"head_delay_ms")
                .and_then(|x| x.as_f64())
                .unwrap_or_default();

            let median_latency_ms = conn
                .get(&"median_latency_ms")
                .and_then(|x| x.as_f64())
                .unwrap_or_default();

            let peak_latency_ms = conn
                .get(&"peak_latency_ms")
                .and_then(|x| x.as_f64())
                .unwrap_or_default();

            let weighted_latency_ms = conn
                .get(&"weighted_latency_ms")
                .and_then(|x| x.as_f64())
                .unwrap_or_default();

            let x = BackendRpcData {
                active_requests,
                backup,
                block_data_limit,
                head_block,
                head_delay_ms,
                median_latency_ms,
                name,
                peak_latency_ms,
                tier,
                total_requests,
                weighted_latency_ms,
            };

            all_requests += x.total_requests;

            rpc_data.push(x);
        }

        rpc_data.sort_by_key(|x| (Reverse(x.total_requests), OrderedFloat(x.median_latency_ms)));

        let mut table = Table::new();

        table.add_row(row![
            "name",
            "request %",
            "requests",
            "active",
            "lag",
            "block_data_limit",
            "head_ms",
            "median_ms",
            "peak_ms",
            "weighted_ms",
            "tier",
        ]);

        for rpc in rpc_data.into_iter() {
            let request_pct = if all_requests == 0 {
                0.0
            } else {
                (rpc.total_requests as f32) / (all_requests as f32) * 100.0
            };

            let block_data_limit = if rpc.block_data_limit == u64::MAX {
                "archive".to_string()
            } else {
                format!("{}", rpc.block_data_limit)
            };

            let tier = if rpc.backup {
                format!("{}B", rpc.tier)
            } else {
                rpc.tier.to_string()
            };

            let lag = highest_block - rpc.head_block;

            table.add_row(row![
                rpc.name,
                format!("{:.3}", request_pct),
                rpc.total_requests,
                rpc.active_requests,
                lag,
                block_data_limit,
                format!("{:.3}", rpc.head_delay_ms),
                rpc.median_latency_ms,
                rpc.peak_latency_ms,
                format!("{:.3}", rpc.weighted_latency_ms),
                tier,
            ]);
        }

        table.printstd();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::head_block_number;
    use web3_proxy::prelude::sonic_rs::{json, JsonContainerTrait};

    #[test]
    fn head_block_number_requires_a_u64_status_field() {
        let numeric = json!({"head_block": {"block": {"number": 18_173_997}}});
        let legacy_quantity = json!({"head_block": {"block": {"number": "0x1154fad"}}});

        assert_eq!(head_block_number(numeric.as_object().unwrap()), 18_173_997);
        assert_eq!(head_block_number(legacy_quantity.as_object().unwrap()), 0);
    }
}
