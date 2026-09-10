use super::*;
use crate::block_relay::journal::{Pending, StateStore};

#[tokio::test]
async fn journal_survives_restart_and_normalizes_endpoint_identity() {
    let directory = tempfile::tempdir().unwrap();
    let pending = Pending {
        hash: B256::with_last_byte(9),
        number: 9,
    };
    let store = StateStore::open(directory.path()).unwrap();
    let journal = store.journal("http://localhost:8551").unwrap();
    assert!(Arc::ptr_eq(
        &journal,
        &store.journal("http://localhost:8551/").unwrap()
    ));
    journal.begin(pending).await.unwrap();
    assert!(journal.begin(pending).await.is_err());
    drop(journal);
    drop(store);
    let store = StateStore::open(directory.path()).unwrap();
    let journal = store.journal("http://localhost:8551/").unwrap();
    assert_eq!(journal.status(), (Some(pending), false));
    assert!(journal
        .complete(Pending {
            number: 10,
            ..pending
        })
        .await
        .is_err());
    assert_eq!(journal.status(), (Some(pending), false));
    journal.complete(pending).await.unwrap();
    drop(journal);
    drop(store);
    let store = StateStore::open(directory.path()).unwrap();
    assert_eq!(
        store.journal("http://localhost:8551").unwrap().status(),
        (None, false)
    );
}

#[tokio::test]
async fn state_lock_is_local_and_journal_keeps_it_until_requests_finish() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let store = StateStore::open(a.path()).unwrap();
    assert!(StateStore::open(a.path()).is_err());
    let other = StateStore::open(b.path()).unwrap();
    let journal = store.journal("http://localhost:8551").unwrap();
    let pending = Pending {
        hash: B256::ZERO,
        number: 1,
    };
    journal.begin(pending).await.unwrap();
    other
        .journal("http://localhost:8551")
        .unwrap()
        .begin(pending)
        .await
        .unwrap();
    drop(store);
    assert!(StateStore::open(a.path()).is_err());
    drop(journal);
    assert!(StateStore::open(a.path()).is_ok());
}

#[tokio::test]
async fn corrupt_journal_fails_closed_and_records_do_not_contain_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let store = StateStore::open(directory.path()).unwrap();
    let endpoint = "http://secret-name:secret-password@localhost:8551/secret-path";
    let journal = store.journal(endpoint).unwrap();
    journal
        .begin(Pending {
            hash: B256::ZERO,
            number: 1,
        })
        .await
        .unwrap();
    let path = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "json"))
        .unwrap();
    assert!(!path.to_string_lossy().contains("secret"));
    assert!(!std::fs::read_to_string(&path).unwrap().contains("secret"));
    drop(journal);
    drop(store);
    for invalid in [
        "{",
        "{}",
        "{\"state\":\"pending\"}",
        "{\"state\":\"idle\",\"hash\":1}",
    ] {
        std::fs::write(&path, invalid).unwrap();
        let store = StateStore::open(directory.path()).unwrap();
        assert_eq!(
            store.journal(endpoint).err().unwrap().to_string(),
            "invalid Engine journal"
        );
    }
}

#[tokio::test]
async fn journal_storage_failure_prevents_the_engine_post() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("state");
    let store = StateStore::open(&state_path).unwrap();
    let mut target = rpc.target(&server.url, "a");
    Arc::get_mut(&mut target).unwrap().journal = store.journal(&server.url).unwrap();
    // Remove the pathname without changing the held lock or deleting any data.
    std::fs::rename(&state_path, directory.path().join("moved-state")).unwrap();
    let worker = Worker::start(target, config::Mode::Inject).await;
    worker
        .tx
        .send(work(1, 0, 1, config::Mode::Inject, &["a"]))
        .unwrap();
    until(|| !worker.stats.lock().execution_targets["a"].health.connected).await;
    worker
        .tx
        .send(work(2, 1, 2, config::Mode::Inject, &["a"]))
        .unwrap();
    worker.finish().await;
    assert!(rpc.payload_hashes().is_empty());
}

#[tokio::test]
async fn restart_waits_for_rpc_confirmation_before_submitting_next_block() {
    let rpc = MockRpc::new();
    let server = Server::rpc(rpc.clone()).await;
    let directory = tempfile::tempdir().unwrap();
    let store = StateStore::open(directory.path()).unwrap();
    let journal = store.journal(&server.url).unwrap();
    let pending = Pending {
        hash: B256::with_last_byte(1),
        number: 1,
    };
    journal.begin(pending).await.unwrap();
    drop(journal);
    drop(store);
    let store = StateStore::open(directory.path()).unwrap();
    let mut target = rpc.target(&server.url, "a");
    Arc::get_mut(&mut target).unwrap().journal = store.journal(&server.url).unwrap();
    let worker = Worker::spawn(target, config::Mode::Inject);
    worker
        .tx
        .send(work(2, 1, 2, config::Mode::Inject, &["a"]))
        .unwrap();
    until(|| {
        worker
            .stats
            .lock()
            .execution_targets
            .get("a")
            .is_some_and(|t| t.health.detail.contains("awaiting RPC confirmation"))
    })
    .await;
    assert!(rpc.payload_hashes().is_empty());
    rpc.state.lock().known.insert(pending.hash, pending.number);
    until(|| worker.stats.lock().execution_targets["a"].valid == 1).await;
    worker.finish().await;
    assert_eq!(rpc.payload_hashes(), vec![B256::with_last_byte(2)]);
    assert_eq!(store.journal(&server.url).unwrap().status(), (None, false));
}
