//! Await fixture cleanup before a test returns, including assertion failures.
use futures::{future::BoxFuture, FutureExt};
use std::{cell::RefCell, future::Future, panic::AssertUnwindSafe};
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

tokio::task_local! {
    static CLEANUP: RefCell<Vec<BoxFuture<'static, ()>>>;
}

pub(super) fn on_cleanup(cleanup: impl Future<Output = ()> + Send + 'static) {
    CLEANUP.with(|pending| pending.borrow_mut().push(cleanup.boxed()));
}

pub(super) async fn with_cleanup(test: impl Future<Output = ()>) {
    CLEANUP
        .scope(RefCell::new(Vec::new()), async move {
            let result = AssertUnwindSafe(test).catch_unwind().await;
            let pending = CLEANUP.with(|pending| std::mem::take(&mut *pending.borrow_mut()));
            // Run all shutdowns together: a frontend can still own backend work.
            // Catch each cleanup panic so the other resources still finish.
            let cleaned = futures::future::join_all(
                pending
                    .into_iter()
                    .map(|cleanup| AssertUnwindSafe(cleanup).catch_unwind()),
            )
            .await;
            if let Err(panic) = result {
                std::panic::resume_unwind(panic);
            }
            for result in cleaned {
                if let Err(panic) = result {
                    std::panic::resume_unwind(panic);
                }
            }
        })
        .await;
}

/// A test can join or cancel the call normally. Cleanup also cancels and waits
/// for calls whose handles were dropped while unwinding an assertion failure.
pub(super) fn spawn<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    let tracker = TaskTracker::new();
    let task = tracker.spawn(future);
    let abort = task.abort_handle();
    on_cleanup(async move {
        abort.abort();
        tracker.close();
        tracker.wait().await;
    });
    task
}
