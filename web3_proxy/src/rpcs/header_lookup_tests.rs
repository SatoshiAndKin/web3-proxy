//! Concurrent cold-header lookups must share work without sharing caller lifetimes.
use super::{
    batch_tests::{Harness, Incoming},
    test_support,
};
use alloy::primitives::B256;
use serde_json::json;
use tokio::time::{timeout, Duration};

fn answer_header(incoming: Incoming) {
    let mut block: alloy::rpc::types::Block = Default::default();
    block.header.hash = incoming.body["params"][0]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    block.header.inner.number = 41;
    let id = incoming.body["id"].clone();
    incoming.respond(json!({"jsonrpc":"2.0","id":id,"result":block}));
}

#[tokio::test]
async fn same_hash_lookup_is_shared_while_other_hashes_progress() {
    test_support::with_cleanup(async {
        let mut h = Harness::new(128, 64).await;
        let hash = B256::repeat_byte(1);
        let pool = h.app.balanced_rpcs.clone();
        let first = test_support::spawn(async move { pool.block_header_by_hash(hash).await });
        let pending = h.next().await;
        let mut followers = Vec::new();
        for _ in 0..32 {
            let pool = h.app.balanced_rpcs.clone();
            followers.push(test_support::spawn(async move {
                pool.block_header_by_hash(hash).await
            }));
        }
        h.quiet().await;
        let other_hash = B256::repeat_byte(2);
        let pool = h.app.balanced_rpcs.clone();
        let other = test_support::spawn(async move { pool.block_header_by_hash(other_hash).await });
        let independent = h.next().await;
        assert_eq!(independent.body["params"][0], format!("{other_hash:#x}"));
        answer_header(independent);
        assert_eq!(*other.await.unwrap().unwrap().hash(), other_hash);
        answer_header(pending);
        assert_eq!(*first.await.unwrap().unwrap().hash(), hash);
        for follower in followers {
            assert_eq!(*follower.await.unwrap().unwrap().hash(), hash);
        }
        assert_eq!(
            h.rpc
                .total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        h.idle();
    })
    .await;
}

#[tokio::test]
async fn cancelling_lookup_owner_allows_a_waiter_to_finish() {
    test_support::with_cleanup(async {
        let mut h = Harness::new(8, 64).await;
        let hash = B256::repeat_byte(3);
        let pool = h.app.balanced_rpcs.clone();
        let first = test_support::spawn(async move { pool.block_header_by_hash(hash).await });
        let abandoned = h.next().await;
        let pool = h.app.balanced_rpcs.clone();
        let waiter = test_support::spawn(async move { pool.block_header_by_hash(hash).await });
        h.quiet().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        drop(abandoned);
        answer_header(h.next().await);
        assert_eq!(*waiter.await.unwrap().unwrap().hash(), hash);
        h.idle();
    })
    .await;
}

#[tokio::test]
async fn waiter_deadline_and_lookup_failure_do_not_poison_the_cache() {
    test_support::with_cleanup(async {
        let mut h = Harness::new(8, 64).await;
        let hash = B256::repeat_byte(4);
        let pool = h.app.balanced_rpcs.clone();
        let first = test_support::spawn(async move { pool.block_header_by_hash(hash).await });
        let pending = h.next().await;
        assert!(timeout(
            Duration::from_millis(20),
            h.app.balanced_rpcs.block_header_by_hash(hash)
        )
        .await
        .is_err());
        h.quiet().await;
        let id = pending.body["id"].clone();
        pending.respond(json!({"jsonrpc":"2.0","id":id,"result":null}));
        assert!(first.await.unwrap().is_err());
        let pool = h.app.balanced_rpcs.clone();
        let retry = test_support::spawn(async move { pool.block_header_by_hash(hash).await });
        answer_header(h.next().await);
        assert_eq!(*retry.await.unwrap().unwrap().hash(), hash);
        h.quiet().await;
        h.idle();
    })
    .await;
}
