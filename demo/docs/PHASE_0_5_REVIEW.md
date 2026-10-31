# Phase 0.5 review — `execute_with_metadata` in cqrsx

Commit: `5407f78 Add execute_with_metadata to the CommandExecutor port` (the only cqrsx change in the
whole project; revert with `git revert 5407f78`). Root `Cargo.lock` unchanged (no new dependencies).

## What changed

| File | Change |
|---|---|
| `cqrsx/src/cqrs/command.rs` | `CommandExecutor` gains `execute_with_metadata(&self, aggregate_id, command, metadata: HashMap<String, String>, tasks)`; `execute` becomes a default method delegating with `HashMap::new()`. Doc comment explains the contract; the `compile_fail` doctest now implements `execute_with_metadata` with non-`Send` state across an await, so it still fails for the original reason. |
| `cqrsx/src/cqrs/command/executor.rs` | `CqrsCommandExecutor::execute_with_metadata` builds the framework once and hands `metadata.clone()` to `CqrsFramework::execute_with_metadata` on every conflict attempt; inherent `execute` delegates with an empty map; the trait impl implements only `execute_with_metadata`. Tests: `metadata_reaches_the_commit_and_stays_identical_across_conflict_retries` (two conflicts → three commits with the same map), `execute_without_metadata_commits_an_empty_map_through_both_entry_points` (trait default and inherent `execute`), `BorrowingExecutor` now asserts the default passes an empty map. |
| `cqrsx/src/postgres/command.rs` | `PostgresCommandExecutor::execute_with_metadata` (forwards to the engine); inherent `execute` delegates; trait impl implements `execute_with_metadata`. |
| `cqrsx/tests/postgres/command.rs` | `command_metadata_is_stored_with_the_event_and_defaults_to_an_empty_object`: `events.metadata` equals the passed map, a later `execute` stores `{}`, a rejected command with metadata stores nothing, the task row still points at the event's sequence. |
| `cqrsx/doc/components.md` | Command-boundary row mentions `execute_with_metadata` / default `execute`. |

Return types are unchanged; `tasks` semantics are unchanged (bound once per invocation).

## Verification (root `CLAUDE.md` set)

Run on 2026-10-17 in the sandbox. Integration tests ran through the Podman API shim described in
`PROGRESS.md` (host-network containers, unchanged test code). The first full run had one failure,
`pool::connection_helper_preserves_ten_connection_limit_and_returns_authentication_error`, because the
postgres image trusts `127.0.0.1` by default and host-network clients are local; the shim now passes
`POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256`, which restores the bridge-network behaviour. The rerun
of the four affected commands is what the table shows for them.

| Command | Exit | Tail of output |
|---|---|---|
| `cargo fmt --all --check` | 0 | <pre></pre> |
| `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` | 0 | <pre>Checking cqrsx v0.1.0 (/workspace/event-sourcing/cqrsx)<br>    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.95s</pre> |
| `cargo test --locked --workspace --all-features` | 0 | <pre>test cqrsx/src/task/queue.rs - task::queue::TaskQueue (line 24) - compile fail ... ok<br>test cqrsx/src/task/handler.rs - task::handler::TaskHandler (line 56) - compile fail ... ok<br><br>test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.18s<br><br>all doctests ran in 1.15s; merged doctests compilation took 0.95s</pre> |
| `cargo test --locked -p cqrsx --no-default-features` | 0 | <pre>test cqrsx/src/task/handler.rs - task::handler::TaskHandler (line 56) - compile fail ... ok<br>test cqrsx/src/task/queue.rs - task::queue::TaskQueue (line 24) - compile fail ... ok<br><br>test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.11s<br><br>all doctests ran in 0.94s; merged doctests compilation took 0.82s</pre> |
| `cargo test --locked -p cqrsx --no-default-features --features postgres` | 0 | <pre>test cqrsx/src/task/handler.rs - task::handler::TaskHandler (line 56) - compile fail ... ok<br>test cqrsx/src/task/queue.rs - task::queue::TaskQueue (line 24) - compile fail ... ok<br><br>test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.12s<br><br>all doctests ran in 1.07s; merged doctests compilation took 0.94s</pre> |
| `cargo test --locked -p cqrsx --no-default-features --features worker` | 0 | <pre>test cqrsx/src/task/handler.rs - task::handler::TaskHandler (line 56) - compile fail ... ok<br>test cqrsx/src/task/queue.rs - task::queue::TaskQueue (line 24) - compile fail ... ok<br><br>test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s<br><br>all doctests ran in 0.93s; merged doctests compilation took 0.82s</pre> |
| `cargo test --locked -p cqrsx --no-default-features --features host` | 0 | <pre>test cqrsx/src/task/handler.rs - task::handler::TaskHandler (line 30) - compile fail ... ok<br>test cqrsx/src/task/queue.rs - task::queue::TaskQueue (line 24) - compile fail ... ok<br><br>test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s<br><br>all doctests ran in 0.94s; merged doctests compilation took 0.82s</pre> |
| `cargo test --locked -p cqrsx --no-default-features --features logging` | 0 | <pre>test cqrsx/src/task/handler.rs - task::handler::TaskHandler (line 56) - compile fail ... ok<br>test cqrsx/src/task/queue.rs - task::queue::TaskQueue (line 24) - compile fail ... ok<br><br>test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.10s<br><br>all doctests ran in 0.95s; merged doctests compilation took 0.83s</pre> |
| `cargo llvm-cov --locked --workspace --all-features --fail-under-lines 97` | 0 | <pre>task/runtime/worker.rs                         264                 8    96.97%          10                 0   100.00%         139                 3    97.84%           0                 0         -<br>task/runtime/worker_pool.rs                    259                 1    99.61%          22                 0   100.00%         182                 1    99.45%           0                 0         -<br>task/runtime/worker_pool_config.rs             406                 8    98.03%          32                 0   100.00%         256                 2    99.22%           0                 0         -<br>task/test_support.rs                           186                 7    96.24%          19                 2    89.47%         116                 6    94.83%           0                 0         -<br>------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------<br>TOTAL                                        12525               357    97.15%        1086                57    94.75%        8435               206    97.56%           0                 0         -</pre> |
| `cargo llvm-cov --locked -p cqrsx --all-features --fail-under-lines 97` | 0 | <pre>task/runtime/worker.rs                         264                 8    96.97%          10                 0   100.00%         139                 3    97.84%           0                 0         -<br>task/runtime/worker_pool.rs                    259                 1    99.61%          22                 0   100.00%         182                 1    99.45%           0                 0         -<br>task/runtime/worker_pool_config.rs             406                 8    98.03%          32                 0   100.00%         256                 2    99.22%           0                 0         -<br>task/test_support.rs                           186                 7    96.24%          19                 2    89.47%         116                 6    94.83%           0                 0         -<br>------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------<br>TOTAL                                        12525               357    97.15%        1086                57    94.75%        8435               206    97.56%           0                 0         -</pre> |

Coverage summary line (both runs): `TOTAL ... 8435 lines, 206 missed, 97.56%` (threshold 97 %).

## Full diff

```diff
diff --git a/cqrsx/doc/components.md b/cqrsx/doc/components.md
index 638450a..93d9f8a 100644
--- a/cqrsx/doc/components.md
+++ b/cqrsx/doc/components.md
@@ -40,7 +40,7 @@ Related configuration and validation errors stay with their owning boundary.
 | Event emission | `EventSink` | Apply events to the working aggregate immediately and buffer them before persistence. |
 | Command orchestration | `CqrsFramework`, `AggregateError` | Load committed state, handle a command on a copy, commit, then invoke event handlers. `Domain` is a business rejection. |
 | Post-commit failures | `CommittedWithEventHandlerFailure`, `EventHandlerFailure` | Record the committed sequence, registration indices and original handler failures without retrying the command. |
-| Command boundary | `CommandExecutor`, `CommandGatewayError` | Technical execution port and safe domain/conflict/unavailable/committed outcome categories. |
+| Command boundary | `CommandExecutor`, `CommandGatewayError` | Technical execution port and safe domain/conflict/unavailable/committed outcome categories. `execute_with_metadata` stores one metadata map with every committed event; `execute` defaults to an empty map. |
 | Command implementation | `CqrsCommandExecutor`, `BuildFramework` callback | One framework per invocation with immutable prebuilt tasks and bounded conflict retry. The callback is a factory. |
 | Conflict retry | `AggregateConflictRetryPolicy`, `AggregateConflictRetryPolicyError`, `retry_on_aggregate_conflict` | Repeat only confirmed optimistic conflicts; independent of durable task redelivery. |
 | Event store | `EventStore`, `AggregateContext`, `PersistenceError` | Typed reconstruction and commit against the last committed context. |
diff --git a/cqrsx/src/cqrs/command.rs b/cqrsx/src/cqrs/command.rs
index f04f7b2..05e5907 100644
--- a/cqrsx/src/cqrs/command.rs
+++ b/cqrsx/src/cqrs/command.rs
@@ -1,6 +1,6 @@
 //! Command gateways with bounded optimistic-conflict retry.
 use crate::{cqrs::aggregate::Aggregate, task::NewTask};
-use std::future::Future;
+use std::{collections::HashMap, future::Future};
 use thiserror::Error;
 pub mod executor;
 pub mod retry;
@@ -9,16 +9,27 @@ pub mod retry;
 ///
 /// Implementations own persistence and retry policy without interpreting domain
 /// rejections. Tasks belong to this invocation and must not leak into other calls.
-/// Implement with an ordinary `async fn`, without `async_trait`. The future must
-/// be `Send` and may borrow the aggregate ID through awaits. Commands require
-/// `Send`; the CQRS retry engine additionally needs `Clone`, never `Sync`.
+/// Event metadata travels with the invocation as well: every event the command
+/// commits stores the same map, and [`execute`](Self::execute) is the
+/// metadata-free form that delegates with an empty map.
+/// Implement [`execute_with_metadata`](Self::execute_with_metadata) with an
+/// ordinary `async fn`, without `async_trait`. The future must be `Send` and may
+/// borrow the aggregate ID through awaits. Commands require `Send`; the CQRS
+/// retry engine additionally needs `Clone`, never `Sync`.
 /// A future holding non-Send state across await violates the contract:
 /// ```compile_fail
 /// use cqrsx::cqrs::aggregate::Aggregate;
 /// use cqrsx::{cqrs::command::{CommandExecutor, CommandGatewayError}, task::NewTask};
+/// use std::collections::HashMap;
 /// struct NonSendExecutor;
 /// impl<A: Aggregate> CommandExecutor<A> for NonSendExecutor where A::Command: Send {
-///     async fn execute(&self, _: &str, _: A::Command, _: Vec<NewTask>) -> Result<(), CommandGatewayError<A::Error>> {
+///     async fn execute_with_metadata(
+///         &self,
+///         _: &str,
+///         _: A::Command,
+///         _: HashMap<String, String>,
+///         _: Vec<NewTask>,
+///     ) -> Result<(), CommandGatewayError<A::Error>> {
 ///         let state = std::rc::Rc::new(());
 ///         std::future::pending::<()>().await;
 ///         drop(state);
@@ -31,11 +42,24 @@ where
     A: Aggregate,
     A::Command: Send,
 {
+    /// Executes without event metadata. The default delegates to
+    /// [`execute_with_metadata`](Self::execute_with_metadata) with an empty map.
     fn execute(
         &self,
         aggregate_id: &str,
         command: A::Command,
         tasks: Vec<NewTask>,
+    ) -> impl Future<Output = Result<(), CommandGatewayError<A::Error>>> + Send {
+        self.execute_with_metadata(aggregate_id, command, HashMap::new(), tasks)
+    }
+
+    /// Executes one command and stores `metadata` with every committed event.
+    fn execute_with_metadata(
+        &self,
+        aggregate_id: &str,
+        command: A::Command,
+        metadata: HashMap<String, String>,
+        tasks: Vec<NewTask>,
     ) -> impl Future<Output = Result<(), CommandGatewayError<A::Error>>> + Send;
 }
 
diff --git a/cqrsx/src/cqrs/command/executor.rs b/cqrsx/src/cqrs/command/executor.rs
index f01d1f8..b0d0b75 100644
--- a/cqrsx/src/cqrs/command/executor.rs
+++ b/cqrsx/src/cqrs/command/executor.rs
@@ -5,15 +5,15 @@ use crate::{
     },
     task::NewTask,
 };
-use std::{error::Error, marker::PhantomData};
+use std::{collections::HashMap, error::Error, marker::PhantomData};
 /// Executes commands for one aggregate type through frameworks supplied by the
 /// composition root or a runtime adapter factory.
 ///
-/// The framework is built exactly once per [`execute`](Self::execute) call.
-/// Every optimistic-conflict attempt reloads aggregate state through that same
-/// framework and repeats clones of the original command. The immutable outbox
-/// task list is handed to the factory once and therefore cannot be regenerated
-/// or changed between attempts.
+/// The framework is built exactly once per [`execute_with_metadata`](Self::execute_with_metadata)
+/// call. Every optimistic-conflict attempt reloads aggregate state through that
+/// same framework and repeats clones of the original command and of its event
+/// metadata. The immutable outbox task list is handed to the factory once and
+/// therefore cannot be regenerated or changed between attempts.
 pub struct CqrsCommandExecutor<A, BuildFramework> {
     build_framework: BuildFramework,
     retry_policy: AggregateConflictRetryPolicy,
@@ -35,12 +35,31 @@ where
     A: Aggregate + Clone,
     A::Command: Clone + Send,
 {
+    /// Executes without event metadata; see [`execute_with_metadata`](Self::execute_with_metadata).
     pub async fn execute<ES>(
         &self,
         aggregate_id: &str,
         command: A::Command,
         tasks: Vec<NewTask>,
     ) -> Result<(), CommandGatewayError<A::Error>>
+    where
+        BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync,
+        ES: EventStore<A> + 'static,
+        ES::AC: Send,
+    {
+        self.execute_with_metadata(aggregate_id, command, HashMap::new(), tasks).await
+    }
+
+    /// Builds one framework for `tasks`, then runs the command with bounded
+    /// conflict retry. Each attempt receives a clone of `metadata`, so a retried
+    /// command commits exactly the metadata of its first attempt.
+    pub async fn execute_with_metadata<ES>(
+        &self,
+        aggregate_id: &str,
+        command: A::Command,
+        metadata: HashMap<String, String>,
+        tasks: Vec<NewTask>,
+    ) -> Result<(), CommandGatewayError<A::Error>>
     where
         BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync,
         ES: EventStore<A> + 'static,
@@ -48,8 +67,9 @@ where
     {
         let framework = (self.build_framework)(tasks);
         let framework_ref = &framework;
+        let metadata_ref = &metadata;
         retry_on_aggregate_conflict(self.retry_policy, move || {
-            framework_ref.execute(aggregate_id, command.clone())
+            framework_ref.execute_with_metadata(aggregate_id, command.clone(), metadata_ref.clone())
         })
         .await
         .inspect_err(|error| log_aggregate_error(A::TYPE, error))
@@ -82,13 +102,14 @@ where
     ES: EventStore<A> + 'static,
     ES::AC: Send,
 {
-    async fn execute(
+    async fn execute_with_metadata(
         &self,
         aggregate_id: &str,
         command: A::Command,
+        metadata: HashMap<String, String>,
         tasks: Vec<NewTask>,
     ) -> Result<(), CommandGatewayError<A::Error>> {
-        CqrsCommandExecutor::execute(self, aggregate_id, command, tasks).await
+        CqrsCommandExecutor::execute_with_metadata(self, aggregate_id, command, metadata, tasks).await
     }
 }
 
@@ -289,10 +310,11 @@ mod tests {
     struct BorrowingExecutor;
 
     impl CommandExecutor<SendOnlyAggregate> for BorrowingExecutor {
-        async fn execute(
+        async fn execute_with_metadata(
             &self,
             aggregate_id: &str,
             command: std::cell::Cell<u32>,
+            metadata: HashMap<String, String>,
             tasks: Vec<NewTask>,
         ) -> Result<(), CommandGatewayError<DummyError>> {
             let address = aggregate_id.as_ptr() as usize;
@@ -301,6 +323,7 @@ mod tests {
             assert_eq!(aggregate_id, "borrowed-id");
             assert_eq!(aggregate_id.as_ptr() as usize, address);
             assert_eq!(command.get(), 8);
+            assert!(metadata.is_empty(), "the default execute must pass an empty map");
             assert!(tasks.is_empty());
             Ok(())
         }
@@ -317,6 +340,112 @@ mod tests {
         assert_eq!(aggregate_id, "borrowed-id");
     }
 
+    /// Records the metadata of every commit and conflicts a configured number of times first.
+    #[derive(Clone)]
+    struct MetadataStore {
+        conflicts: usize,
+        commits: Arc<Mutex<Vec<HashMap<String, String>>>>,
+    }
+
+    impl EventStore<DummyAggregate> for MetadataStore {
+        type AC = DummyContext;
+
+        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
+            Ok(Vec::new())
+        }
+
+        async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<DummyError>> {
+            Ok(DummyContext(DummyAggregate))
+        }
+
+        async fn commit(
+            &self,
+            _: Vec<DummyEvent>,
+            _: Self::AC,
+            metadata: HashMap<String, String>,
+        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
+            let mut commits = self.commits.lock().unwrap();
+            commits.push(metadata);
+            if commits.len() <= self.conflicts {
+                Err(AggregateError::AggregateConflict)
+            } else {
+                Ok(Vec::new())
+            }
+        }
+    }
+
+    fn metadata_executor(
+        conflicts: usize,
+        commits: &Arc<Mutex<Vec<HashMap<String, String>>>>,
+    ) -> impl CommandExecutor<DummyAggregate> {
+        let commits = commits.clone();
+        CqrsCommandExecutor::<DummyAggregate, _>::new(
+            move |_| {
+                CqrsFramework::new(
+                    MetadataStore {
+                        conflicts,
+                        commits: commits.clone(),
+                    },
+                    vec![],
+                    Arc::default(),
+                )
+            },
+            no_delay_policy(3),
+        )
+    }
+
+    #[tokio::test]
+    async fn metadata_reaches_the_commit_and_stays_identical_across_conflict_retries() {
+        let commits = Arc::new(Mutex::new(Vec::new()));
+        let executor = metadata_executor(2, &commits);
+        let metadata = HashMap::from([
+            ("command_id".to_owned(), "cmd-1".to_owned()),
+            ("source".to_owned(), "ui".to_owned()),
+        ]);
+        executor
+            .execute_with_metadata(
+                "aggregate",
+                DummyCommand::Record("value".to_owned()),
+                metadata.clone(),
+                vec![],
+            )
+            .await
+            .unwrap();
+        assert_eq!(*commits.lock().unwrap(), vec![metadata.clone(), metadata.clone(), metadata]);
+    }
+
+    #[tokio::test]
+    async fn execute_without_metadata_commits_an_empty_map_through_both_entry_points() {
+        let commits = Arc::new(Mutex::new(Vec::new()));
+        let executor = metadata_executor(0, &commits);
+        CommandExecutor::execute(&executor, "aggregate", DummyCommand::Record("trait".to_owned()), vec![])
+            .await
+            .unwrap();
+        let commits_inherent = Arc::new(Mutex::new(Vec::new()));
+        let inherent = CqrsCommandExecutor::<DummyAggregate, _>::new(
+            {
+                let commits = commits_inherent.clone();
+                move |_| {
+                    CqrsFramework::new(
+                        MetadataStore {
+                            conflicts: 0,
+                            commits: commits.clone(),
+                        },
+                        vec![],
+                        Arc::default(),
+                    )
+                }
+            },
+            no_delay_policy(1),
+        );
+        inherent
+            .execute("aggregate", DummyCommand::Record("inherent".to_owned()), vec![])
+            .await
+            .unwrap();
+        assert_eq!(*commits.lock().unwrap(), vec![HashMap::new()]);
+        assert_eq!(*commits_inherent.lock().unwrap(), vec![HashMap::new()]);
+    }
+
     #[tokio::test]
     async fn executor_builds_once_and_retries_the_unchanged_command_and_tasks() {
         let builds = Arc::new(AtomicUsize::new(0));
diff --git a/cqrsx/src/postgres/command.rs b/cqrsx/src/postgres/command.rs
index 7644516..8210259 100644
--- a/cqrsx/src/postgres/command.rs
+++ b/cqrsx/src/postgres/command.rs
@@ -8,7 +8,7 @@ use crate::{
     postgres::outbox::PostgresEventRepositoryWithOutbox,
 };
 use sqlx::PgPool;
-use std::{marker::PhantomData, num::NonZeroUsize};
+use std::{collections::HashMap, marker::PhantomData, num::NonZeroUsize};
 use thiserror::Error;
 
 /// Checked snapshot interval and optimistic-conflict policy. Does not read env.
@@ -99,12 +99,24 @@ where
     A::Command: Clone + Send + 'static,
     A::Error: Send + 'static,
 {
-    /// Execute through the common engine, retrying only optimistic conflicts.
+    /// Execute without event metadata; see [`execute_with_metadata`](Self::execute_with_metadata).
     pub async fn execute(
         &self,
         aggregate_id: &str,
         command: A::Command,
         tasks: Vec<crate::task::NewTask>,
+    ) -> Result<(), CommandGatewayError<A::Error>> {
+        self.execute_with_metadata(aggregate_id, command, HashMap::new(), tasks).await
+    }
+
+    /// Execute through the common engine, retrying only optimistic conflicts.
+    /// `metadata` is stored in the `metadata` column of the committed event.
+    pub async fn execute_with_metadata(
+        &self,
+        aggregate_id: &str,
+        command: A::Command,
+        metadata: HashMap<String, String>,
+        tasks: Vec<crate::task::NewTask>,
     ) -> Result<(), CommandGatewayError<A::Error>> {
         let snapshot_interval = self.config.snapshot_interval();
         let executor = CqrsCommandExecutor::<A, _>::new(
@@ -115,7 +127,7 @@ where
             },
             self.config.conflict_retry(),
         );
-        executor.execute(aggregate_id, command, tasks).await
+        executor.execute_with_metadata(aggregate_id, command, metadata, tasks).await
     }
 }
 
@@ -125,13 +137,14 @@ where
     A::Command: Clone + Send + 'static,
     A::Error: Send + 'static,
 {
-    async fn execute(
+    async fn execute_with_metadata(
         &self,
         aggregate_id: &str,
         command: A::Command,
+        metadata: HashMap<String, String>,
         tasks: Vec<crate::task::NewTask>,
     ) -> Result<(), CommandGatewayError<A::Error>> {
-        PostgresCommandExecutor::execute(self, aggregate_id, command, tasks).await
+        PostgresCommandExecutor::execute_with_metadata(self, aggregate_id, command, metadata, tasks).await
     }
 }
 
diff --git a/cqrsx/tests/postgres/command.rs b/cqrsx/tests/postgres/command.rs
index 71b93da..b2e54c5 100644
--- a/cqrsx/tests/postgres/command.rs
+++ b/cqrsx/tests/postgres/command.rs
@@ -211,6 +211,40 @@ async fn factory_publishes_atomic_work_and_preserves_rejections_and_rollbacks()
     db.close().await;
 }
 
+#[tokio::test]
+async fn command_metadata_is_stored_with_the_event_and_defaults_to_an_empty_object() {
+    let db = Database::new().await;
+    let executor = command_executor::<CommandAggregate>(db.pool.clone(), config(1));
+    let metadata = HashMap::from([
+        ("command_id".to_owned(), "cmd-1".to_owned()),
+        ("source".to_owned(), "integration".to_owned()),
+    ]);
+    let task = new_task();
+    CommandExecutor::execute_with_metadata(
+        &executor,
+        "metadata",
+        record("first", 0),
+        metadata.clone(),
+        vec![task.clone()],
+    )
+    .await
+    .unwrap();
+    executor.execute("metadata", record("second", 1), vec![]).await.unwrap();
+    assert_eq!(
+        executor
+            .execute_with_metadata("metadata", Command::Reject, metadata.clone(), vec![new_task()])
+            .await,
+        Err(CommandGatewayError::Domain(Rejected))
+    );
+    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
+    let events = repository.get_events::<CommandAggregate>("metadata").await.unwrap();
+    assert_eq!(events.len(), 2, "a rejected command commits neither event nor metadata");
+    assert_eq!(events[0].metadata, json!({"command_id": "cmd-1", "source": "integration"}));
+    assert_eq!(events[1].metadata, json!({}));
+    assert_eq!(row(&db.pool, task.id()).await.get::<i64, _>("aggregate_sequence"), 1);
+    db.close().await;
+}
+
 #[tokio::test]
 async fn shared_executor_isolates_parallel_command_task_lists() {
     let db = Database::new().await;
```
