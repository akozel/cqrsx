//! Checked format registration and decoding for one versioned task operation.

use crate::task::{
    adapter::TaskAdapter,
    context::TaskContext,
    decode_with_labels,
    dependencies::DependencyId,
    format::{TaskFormat, TaskPayload},
    handler::TaskHandler,
    result::TaskResult,
};
use serde::de::DeserializeOwned;
use std::{fmt, marker::PhantomData};

/// Adapt a typed operation to the dispatcher and executor interface.
///
/// Checks `P`'s format during construction. Each delivery must match that format
/// and deserialize successfully before the operation is called. Structural
/// failures fail the task, with sanitized expected type/version diagnostics;
/// payloads, metadata, actual arbitrary type names and serde errors are omitted.
/// Decoding reads the JSON payload in place, and a label (`#[label]`) from the
/// metadata, copying neither. Unknown field handling follows `P`'s serde settings.
/// Business results pass unchanged.
///
/// One handler serves every delivery and every worker; its dependencies are the
/// handler's ([`TaskHandler::DEPENDENCIES`]). Register separate adapters for separate
/// DTO versions, or use [`TaskAdapter`] for a custom multi-version handler. This
/// adapter requires no optional runtime feature.
pub struct JsonTaskAdapter<P, H> {
    handler: H,
    format: TaskFormat,
    // `fn() -> P`: the adapter makes payloads, it holds none, so it is `Sync` whatever `P` is.
    payload: PhantomData<fn() -> P>,
}

// By hand: the handler need not be `Debug`.
impl<P, H> fmt::Debug for JsonTaskAdapter<P, H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonTaskAdapter")
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

impl<P, H> JsonTaskAdapter<P, H>
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
    H: TaskHandler<P>,
{
    /// The adapter of `handler` for the payloads of `P`, without I/O, environment lookup or type erasure; `P`'s format
    /// is checked when this compiles ([`TaskFormat::of`]).
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            format: TaskFormat::of::<P>(),
            payload: PhantomData,
        }
    }

    fn safe_format(&self) -> String {
        let task_type = self.format.task_type();
        let safe_type = crate::diagnostics::safe_type(task_type);
        format!("{safe_type} v{}", self.format.task_version())
    }
}

impl<P, H> TaskAdapter for JsonTaskAdapter<P, H>
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
    H: TaskHandler<P>,
{
    fn formats(&self) -> Vec<TaskFormat> {
        vec![self.format.clone()]
    }

    fn dependencies(&self) -> Vec<DependencyId> {
        H::DEPENDENCIES.to_vec()
    }

    async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
        let delivery = ctx.attempt();
        if delivery.format() != &self.format {
            return TaskResult::fail(format!("unsupported task format for {}", self.safe_format()));
        }
        let decoded = if P::LABELS.is_empty() {
            P::deserialize(delivery.payload())
        } else {
            decode_with_labels(P::LABELS, delivery.payload(), delivery.metadata())
        };
        match decoded {
            Ok(payload) => self.handler.handle(payload, ctx).await,
            Err(_) => TaskResult::fail(format!("invalid {} task payload", self.safe_format())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{attempt::TaskAttempt, context::Executors, dependencies::Dependencies};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        cell::Cell,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };
    use uuid::Uuid;

    // This owned DTO is neither Clone nor Sync; the handler is shared.
    #[derive(Serialize, Deserialize)]
    struct Payload<const VERSION: i32> {
        value: String,
        #[serde(skip)]
        marker: Cell<u8>,
    }

    impl<const VERSION: i32> TaskPayload for Payload<VERSION> {
        const TASK_TYPE: &'static str = "test_operation";
        const TASK_VERSION: i32 = VERSION;
    }

    #[derive(Debug)]
    struct Call {
        value: String,
        count: u32,
        id: Uuid,
        attempt: i32,
        format: TaskFormat,
        payload: serde_json::Value,
        metadata: serde_json::Value,
        original_delivery: bool,
    }

    struct RecordingAdapter {
        calls: Arc<Mutex<Vec<Call>>>,
        count: AtomicU32,
        delivery_address: usize,
        result: TaskResult,
    }

    impl<const VERSION: i32> TaskHandler<Payload<VERSION>> for RecordingAdapter {
        async fn handle(&self, payload: Payload<VERSION>, ctx: &TaskContext) -> TaskResult {
            // Keep both the owned non-Sync DTO and the borrowed context across await.
            tokio::task::yield_now().await;
            payload.marker.set(1);
            let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
            let delivery = ctx.attempt();
            self.calls.lock().unwrap().push(Call {
                value: payload.value,
                count,
                id: delivery.task_id(),
                attempt: delivery.number(),
                format: delivery.format().clone(),
                payload: delivery.payload().clone(),
                metadata: delivery.metadata().clone(),
                original_delivery: std::ptr::from_ref(delivery) as usize == self.delivery_address,
            });
            self.result.clone()
        }
    }

    fn adapter(calls: &Arc<Mutex<Vec<Call>>>, delivery: &TaskAttempt, result: TaskResult) -> RecordingAdapter {
        RecordingAdapter {
            calls: calls.clone(),
            count: AtomicU32::new(0),
            delivery_address: std::ptr::from_ref(delivery) as usize,
            result,
        }
    }

    fn delivery(format: TaskFormat, payload: serde_json::Value) -> Arc<TaskAttempt> {
        Arc::new(TaskAttempt::try_new(Uuid::new_v4(), format, payload, json!({"correlation_id":"original"}), 3).unwrap())
    }

    /// The context of `attempt`, as the dispatcher builds it.
    fn context(attempt: &Arc<TaskAttempt>) -> TaskContext {
        TaskContext::shared(
            attempt.clone(),
            Duration::from_secs(1),
            Dependencies::default(),
            Executors::default(),
        )
    }

    #[tokio::test]
    async fn decodes_owned_dto_preserves_original_delivery_and_shared_handler_state() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let task = delivery(
            TaskFormat::of::<Payload<1>>(),
            json!({"value":"accepted", "future_field":{"locale":"en"}}),
        );
        let adapter = JsonTaskAdapter::<Payload<1>, _>::new(adapter(&calls, &task, TaskResult::Done));
        assert_eq!(adapter.formats(), vec![task.format().clone()]);
        assert_eq!(
            format!("{adapter:?}"),
            r#"JsonTaskAdapter { format: TaskFormat { task_type: "test_operation", task_version: 1 }, .. }"#
        );
        for _ in 0..2 {
            assert_eq!(adapter.invoke(&context(&task)).await, TaskResult::Done);
        }
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for (index, call) in calls.iter().enumerate() {
            assert_eq!(call.value, "accepted");
            assert_eq!(call.count, index as u32 + 1);
            assert_eq!(call.id, task.task_id());
            assert_eq!(call.attempt, task.number());
            assert_eq!(&call.format, task.format());
            assert_eq!(&call.payload, task.payload());
            assert_eq!(&call.metadata, task.metadata());
            assert!(call.original_delivery);
        }
    }

    #[tokio::test]
    async fn format_and_decode_failures_are_permanent_and_never_call_business_handler() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let base = delivery(TaskFormat::of::<Payload<1>>(), json!({"value":"valid"}));
        let adapter = JsonTaskAdapter::<Payload<1>, _>::new(adapter(&calls, &base, TaskResult::Done));
        for (format, payload, reason) in [
            (
                TaskFormat::try_new("private_actual_type\nsecret@example.com", 1).unwrap(),
                json!({"value":"private_payload"}),
                "unsupported task format for test_operation v1",
            ),
            (
                TaskFormat::of::<Payload<2>>(),
                json!({"value":"valid"}),
                "unsupported task format for test_operation v1",
            ),
            // Both wrong format and bad schema: format observation wins.
            (
                TaskFormat::of::<Payload<2>>(),
                json!({}),
                "unsupported task format for test_operation v1",
            ),
            (base.format().clone(), json!({}), "invalid test_operation v1 task payload"),
            (
                base.format().clone(),
                json!({"value":{"private_payload":"secret"}}),
                "invalid test_operation v1 task payload",
            ),
        ] {
            let task = context(&delivery(format, payload));
            let error = adapter.invoke(&task).await;
            assert!(matches!(&error, TaskResult::Fail { reason: actual } if actual == reason));
            assert_eq!(error.to_string(), reason);
            assert!(!format!("{error:?}").contains("private_"));
        }
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn business_results_pass_through_without_retry_or_reason_changes() {
        let task = delivery(TaskFormat::of::<Payload<1>>(), json!({"value":"valid"}));
        for result in [
            TaskResult::Done,
            TaskResult::retry("business retry"),
            TaskResult::fail("business rejection"),
        ] {
            let calls = Arc::new(Mutex::new(Vec::new()));
            let adapter = JsonTaskAdapter::<Payload<1>, _>::new(adapter(&calls, &task, result.clone()));
            assert_eq!(adapter.invoke(&context(&task)).await, result);
            assert_eq!(calls.lock().unwrap().len(), 1);
        }
    }

    #[derive(Serialize)]
    struct PrivatePayload;

    impl<'de> Deserialize<'de> for PrivatePayload {
        fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
            Err(serde::de::Error::custom("private_serde_error_sentinel"))
        }
    }
    impl TaskPayload for PrivatePayload {
        const TASK_TYPE: &'static str = "private_expected_type\nsecret@example.com";
        const TASK_VERSION: i32 = 4;
    }
    struct NeverCalled;
    impl TaskHandler<PrivatePayload> for NeverCalled {
        async fn handle(&self, _: PrivatePayload, _: &TaskContext) -> TaskResult {
            panic!("invalid deliveries must not reach business code")
        }
    }

    #[tokio::test]
    async fn arbitrary_expected_types_and_serde_errors_are_not_exposed() {
        let adapter = JsonTaskAdapter::<PrivatePayload, _>::new(NeverCalled);
        assert_eq!(adapter.formats(), vec![TaskFormat::of::<PrivatePayload>()]);
        for (format, reason) in [
            (TaskFormat::of::<PrivatePayload>(), "invalid unknown v4 task payload"),
            (
                TaskFormat::try_new("other", 4).unwrap(),
                "unsupported task format for unknown v4",
            ),
        ] {
            let task = context(&delivery(format, json!({"private_payload_sentinel":"secret"})));
            let error = adapter.invoke(&task).await;
            assert_eq!(error.to_string(), reason);
            assert!(!format!("{error:?}").contains("private_"));
        }
    }

    /// A payload whose tenant is kept in the task's metadata.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Labelled {
        amount: u32,
        tenant: String,
    }

    impl TaskPayload for Labelled {
        const TASK_TYPE: &'static str = "labelled";
        const TASK_VERSION: i32 = 1;
        const LABELS: &'static [&'static str] = &["tenant"];
    }

    /// Records every payload it decodes.
    struct Decoded(Arc<Mutex<Vec<Labelled>>>);

    impl TaskHandler<Labelled> for Decoded {
        async fn handle(&self, payload: Labelled, _: &TaskContext) -> TaskResult {
            self.0.lock().unwrap().push(payload);
            TaskResult::Done
        }
    }

    #[tokio::test]
    async fn a_labelled_payload_decodes_with_its_labels_from_the_metadata() {
        let decoded = Arc::new(Mutex::new(Vec::new()));
        let adapter = JsonTaskAdapter::<Labelled, _>::new(Decoded(decoded.clone()));
        let published = crate::task::NewTask::try_new(Labelled {
            amount: 80,
            tenant: "acme".into(),
        })
        .unwrap();
        let format = TaskFormat::of::<Labelled>();
        let claim = |payload, metadata| {
            context(&Arc::new(
                TaskAttempt::try_new(Uuid::new_v4(), format.clone(), payload, metadata, 1).unwrap(),
            ))
        };
        assert_eq!(
            adapter
                .invoke(&claim(published.payload().clone(), published.metadata().clone()))
                .await,
            TaskResult::Done
        );
        // A body that still holds the key keeps its own value.
        assert_eq!(
            adapter
                .invoke(&claim(json!({"amount": 5, "tenant": "body"}), json!({"tenant": "metadata"})))
                .await,
            TaskResult::Done
        );
        let missing = adapter.invoke(&claim(json!({"amount": 5}), json!({}))).await;
        assert_eq!(missing.to_string(), "invalid labelled v1 task payload");
        assert_eq!(
            *decoded.lock().unwrap(),
            [
                Labelled {
                    amount: 80,
                    tenant: "acme".into()
                },
                Labelled {
                    amount: 5,
                    tenant: "body".into()
                }
            ]
        );
    }
}
