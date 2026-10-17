//! One adapter type over adapters of different types, so one dispatcher can hold them all.

use crate::task::{
    adapter::{TaskAdapter, json::JsonTaskAdapter},
    attempt::TaskAttempt,
    format::{TaskFormat, TaskFormatError, TaskPayload},
    handler::{TaskHandler, TaskHandlerError},
};
use serde::de::DeserializeOwned;
use std::{future::Future, pin::Pin};

type InvokeFuture<'a> = Pin<Box<dyn Future<Output = Result<(), TaskHandlerError>> + Send + 'a>>;

/// Object-safe shadow of [`TaskAdapter`]: the same contract with a boxed future. Method names
/// differ from the original so the blanket implementation never makes a call ambiguous.
trait DynTaskAdapter: Send {
    fn dyn_formats(&self) -> Vec<TaskFormat>;
    fn dyn_invoke<'a>(&'a mut self, attempt: &'a TaskAttempt) -> InvokeFuture<'a>;
}

impl<T: TaskAdapter> DynTaskAdapter for T {
    fn dyn_formats(&self) -> Vec<TaskFormat> {
        self.formats()
    }

    fn dyn_invoke<'a>(&'a mut self, attempt: &'a TaskAttempt) -> InvokeFuture<'a> {
        Box::pin(self.invoke(attempt))
    }
}

/// Any [`TaskAdapter`] behind one type, so one
/// [`TaskDispatcher`](crate::task::dispatcher::TaskDispatcher) can route to handlers of different
/// types without an application-owned enum.
///
/// Formats and results pass through unchanged; the wrapped adapter keeps its mutable state across
/// invocations. The cost is one allocation per invocation. Wrapped adapters must be `'static`.
pub struct BoxedTaskAdapter(Box<dyn DynTaskAdapter>);

impl BoxedTaskAdapter {
    /// Erase the type of `adapter`.
    pub fn new(adapter: impl TaskAdapter + 'static) -> Self {
        Self(Box::new(adapter))
    }

    /// A [`JsonTaskAdapter`] for `handler`; checks `P`'s format before registration.
    pub fn json<P, H>(handler: H) -> Result<Self, TaskFormatError>
    where
        P: TaskPayload + DeserializeOwned + Send + 'static,
        H: TaskHandler<P> + 'static,
    {
        JsonTaskAdapter::<P, H>::new(handler).map(Self::new)
    }
}

impl TaskAdapter for BoxedTaskAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        self.0.dyn_formats()
    }

    async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.0.dyn_invoke(attempt).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::dispatcher::{TaskDispatcher, TaskDispatcherError};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    use uuid::Uuid;

    #[derive(Serialize, Deserialize)]
    struct Greeting {
        name: String,
    }
    impl TaskPayload for Greeting {
        const TASK_TYPE: &'static str = "greeting";
        const TASK_VERSION: i32 = 1;
    }

    #[derive(Serialize, Deserialize)]
    struct Invoice {
        amount: u32,
    }
    impl TaskPayload for Invoice {
        const TASK_TYPE: &'static str = "invoice";
        const TASK_VERSION: i32 = 2;
    }

    type Calls = Arc<Mutex<Vec<String>>>;

    /// Counts its own invocations in `&mut self` and records them with the decoded payload.
    struct Recorder {
        calls: Calls,
        count: u32,
        result: Result<(), TaskHandlerError>,
    }

    impl Recorder {
        fn new(calls: &Calls) -> Self {
            Self::returning(calls, Ok(()))
        }

        fn returning(calls: &Calls, result: Result<(), TaskHandlerError>) -> Self {
            Self {
                calls: calls.clone(),
                count: 0,
                result,
            }
        }
    }

    impl TaskHandler<Greeting> for Recorder {
        async fn handle(&mut self, payload: Greeting, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            tokio::task::yield_now().await;
            self.count += 1;
            self.calls
                .lock()
                .unwrap()
                .push(format!("greeting {} #{}", payload.name, self.count));
            self.result.clone()
        }
    }

    impl TaskHandler<Invoice> for Recorder {
        async fn handle(&mut self, payload: Invoice, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            self.count += 1;
            self.calls
                .lock()
                .unwrap()
                .push(format!("invoice {} #{}", payload.amount, self.count));
            self.result.clone()
        }
    }

    fn attempt<P: TaskPayload>(payload: serde_json::Value) -> TaskAttempt {
        TaskAttempt::new(Uuid::new_v4(), TaskFormat::of::<P>().unwrap(), payload, json!({}), 1).unwrap()
    }

    #[tokio::test]
    async fn one_dispatcher_routes_differently_typed_adapters_by_format_in_registration_order() {
        let calls = Calls::default();
        let mut dispatcher = TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::json::<Invoice, _>(Recorder::new(&calls)).unwrap(),
            BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&calls)).unwrap(),
        ])
        .unwrap();
        assert_eq!(
            dispatcher.formats(),
            [TaskFormat::of::<Invoice>().unwrap(), TaskFormat::of::<Greeting>().unwrap()]
        );
        for task in [
            attempt::<Greeting>(json!({ "name": "fox" })),
            attempt::<Invoice>(json!({ "amount": 80 })),
            attempt::<Greeting>(json!({ "name": "panda" })),
        ] {
            dispatcher.dispatch(&task).await.unwrap();
        }
        assert_eq!(
            *calls.lock().unwrap(),
            ["greeting fox #1", "invoice 80 #1", "greeting panda #2"],
            "each adapter keeps its own mutable handler state"
        );
    }

    #[test]
    fn duplicate_and_invalid_formats_are_rejected_before_registration() {
        let calls = Calls::default();
        assert!(matches!(
            TaskDispatcher::try_new(vec![
                BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&calls)).unwrap(),
                BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&calls)).unwrap(),
            ]),
            Err(TaskDispatcherError::DuplicateFormat(format)) if format == TaskFormat::of::<Greeting>().unwrap()
        ));

        #[derive(Serialize, Deserialize)]
        struct Unversioned;
        impl TaskPayload for Unversioned {
            const TASK_TYPE: &'static str = "unversioned";
            const TASK_VERSION: i32 = 0;
        }
        struct NeverCalled;
        impl TaskHandler<Unversioned> for NeverCalled {
            async fn handle(&mut self, _: Unversioned, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
                unreachable!("an invalid format cannot construct an adapter")
            }
        }
        assert!(matches!(
            BoxedTaskAdapter::json::<Unversioned, _>(NeverCalled),
            Err(TaskFormatError::InvalidVersion)
        ));
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn handler_results_pass_through_unchanged() {
        for result in [
            Ok(()),
            Err(TaskHandlerError::Retry {
                reason: "later".into(),
                retry_after: Some(Duration::from_secs(7)),
            }),
            Err(TaskHandlerError::Retry {
                reason: "default backoff".into(),
                retry_after: None,
            }),
            Err(TaskHandlerError::Permanent {
                reason: "rejected".into(),
            }),
        ] {
            let calls = Calls::default();
            let mut adapter = BoxedTaskAdapter::json::<Greeting, _>(Recorder::returning(&calls, result.clone())).unwrap();
            let actual = adapter.invoke(&attempt::<Greeting>(json!({ "name": "fox" }))).await;
            assert_eq!(format!("{actual:?}"), format!("{result:?}"));
            assert_eq!(calls.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn a_custom_adapter_keeps_its_formats_and_decoding() {
        /// Accepts two versions of one operation and decodes neither through serde.
        struct MultiVersion {
            seen: Calls,
        }
        impl TaskAdapter for MultiVersion {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![TaskFormat::new("legacy", 1).unwrap(), TaskFormat::new("legacy", 2).unwrap()]
            }
            async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
                self.seen.lock().unwrap().push(attempt.format().to_string());
                Ok(())
            }
        }
        let seen = Calls::default();
        let mut dispatcher = TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::new(MultiVersion { seen: seen.clone() }),
            BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&seen)).unwrap(),
        ])
        .unwrap();
        assert_eq!(dispatcher.formats().len(), 3);
        for version in [2, 1] {
            let task = TaskAttempt::new(
                Uuid::new_v4(),
                TaskFormat::new("legacy", version).unwrap(),
                json!({}),
                json!({}),
                1,
            )
            .unwrap();
            dispatcher.dispatch(&task).await.unwrap();
        }
        assert_eq!(*seen.lock().unwrap(), ["legacy v2", "legacy v1"]);
    }
}
