//! What the task handlers use besides their tasks, found by type.
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt,
    sync::Arc,
};
use thiserror::Error;

/// What the task handlers use besides their tasks, by type: an HTTP client, a mail service, a clock. A handler names
/// a dependency by a parameter of its type (`mail: &Mailer` in a [`#[task_handler]`](crate::task_handler) function);
/// the assembly provides it once for every worker ([`TaskHandlers::provide`](super::handlers::TaskHandlers::provide))
/// and refuses a handler whose dependency nobody provided.
///
/// The type may be a trait object, so a port is a dependency and a test provides its double under the same type:
///
/// ```
/// use cqrsx::task::dependencies::Dependencies;
/// use std::sync::Arc;
///
/// trait MailService: Send + Sync {
///     fn send(&self, to: &str) -> String;
/// }
/// /// The handlers' dependency: the mail service, by this type.
/// type Mailer = dyn MailService;
///
/// struct Recording;
/// impl MailService for Recording {
///     fn send(&self, to: &str) -> String {
///         format!("recorded for {to}")
///     }
/// }
///
/// let mail: Arc<Mailer> = Arc::new(Recording);
/// let dependencies = Dependencies::default().provide(mail);
/// assert_eq!(dependencies.get::<Mailer>().unwrap().send("fox"), "recorded for fox");
/// assert!(dependencies.get::<String>().is_none());
/// ```
///
/// A clone is cheap: the dependencies are shared (`Arc`).
#[derive(Clone, Default)]
#[must_use]
pub struct Dependencies(Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl Dependencies {
    /// `dependency` under its type `T`; a second dependency of a type replaces the first.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(mut self, dependency: Arc<T>) -> Self {
        Arc::make_mut(&mut self.0).insert(TypeId::of::<T>(), Arc::new(dependency));
        self
    }

    /// The dependency of type `T`, if it is provided.
    pub fn get<T: ?Sized + 'static>(&self) -> Option<&T> {
        let stored = self.0.get(&TypeId::of::<T>())?;
        stored.downcast_ref::<Arc<T>>().map(|dependency| &**dependency)
    }

    /// The dependency of type `T`, shared, if it is provided.
    pub(crate) fn shared<T: ?Sized + 'static>(&self) -> Option<Arc<T>> {
        self.0.get(&TypeId::of::<T>())?.downcast_ref::<Arc<T>>().cloned()
    }

    /// These dependencies and `other`'s; a type `other` provides too is `other`'s.
    pub(crate) fn merged(mut self, other: &Dependencies) -> Self {
        Arc::make_mut(&mut self.0).extend(other.0.iter().map(|(id, dependency)| (*id, dependency.clone())));
        self
    }

    /// Whether a dependency of the type `id` names is provided.
    pub fn contains(&self, id: DependencyId) -> bool {
        self.0.contains_key(&id.type_id())
    }
}

impl fmt::Debug for Dependencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dependencies").field("count", &self.0.len()).finish()
    }
}

/// The type of a dependency, usable in a constant: what a handler declares it needs
/// ([`TaskHandler::DEPENDENCIES`](super::handler::TaskHandler::DEPENDENCIES)). Two ids are equal when their types are.
#[derive(Clone, Copy)]
pub struct DependencyId {
    type_id: fn() -> TypeId,
    name: fn() -> &'static str,
}

impl DependencyId {
    /// The id of the type `T`, a trait object too.
    pub const fn of<T: ?Sized + 'static>() -> Self {
        Self {
            type_id: TypeId::of::<T>,
            name: type_name::<T>,
        }
    }

    /// The name of the type, for diagnostics (`dyn app::mail::MailService`).
    pub fn name(&self) -> &'static str {
        (self.name)()
    }

    fn type_id(&self) -> TypeId {
        (self.type_id)()
    }
}

impl PartialEq for DependencyId {
    fn eq(&self, other: &Self) -> bool {
        self.type_id() == other.type_id()
    }
}

impl Eq for DependencyId {}

impl fmt::Debug for DependencyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("DependencyId").field(&self.name()).finish()
    }
}

/// A handler asked for a dependency nobody provided. An assembly refuses such a handler first, so only a context built
/// by hand meets it.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("the dependency `{0}` is not provided")]
pub struct MissingDependency(pub &'static str);

#[cfg(test)]
mod tests {
    use super::*;

    trait Clock: Send + Sync {
        fn now(&self) -> u64;
    }
    type Time = dyn Clock;

    struct Fixed(u64);
    impl Clock for Fixed {
        fn now(&self) -> u64 {
            self.0
        }
    }

    #[test]
    fn a_dependency_is_found_by_its_type_a_trait_object_included_and_a_second_one_replaces_it() {
        let empty = Dependencies::default();
        assert!(empty.get::<Time>().is_none());
        let clock: Arc<Time> = Arc::new(Fixed(7));
        let dependencies = empty.clone().provide(clock).provide(Arc::new(String::from("fox")));
        assert_eq!(dependencies.get::<Time>().map(Clock::now), Some(7));
        assert_eq!(dependencies.get::<String>().map(String::as_str), Some("fox"));
        assert!(
            dependencies.get::<Fixed>().is_none(),
            "the trait object's type, not the implementation's"
        );
        assert!(empty.get::<String>().is_none(), "a clone keeps what it had");
        let later: Arc<Time> = Arc::new(Fixed(8));
        let replaced = dependencies.provide(later);
        assert_eq!(replaced.get::<Time>().map(Clock::now), Some(8));
        assert!(replaced.contains(DependencyId::of::<Time>()));
        assert!(!replaced.contains(DependencyId::of::<u32>()));
        assert_eq!(format!("{replaced:?}"), "Dependencies { count: 2 }");
    }

    #[test]
    fn dependency_ids_compare_by_type_and_name_it() {
        const TIME: DependencyId = DependencyId::of::<Time>();
        assert_eq!(TIME, DependencyId::of::<dyn Clock>());
        assert_ne!(TIME, DependencyId::of::<Fixed>());
        assert!(TIME.name().ends_with("Clock"), "{}", TIME.name());
        assert!(format!("{TIME:?}").starts_with("DependencyId(\"dyn "), "{TIME:?}");
        let missing = MissingDependency(TIME.name());
        assert_eq!(
            missing.to_string(),
            format!("the dependency `{}` is not provided", TIME.name())
        );
    }

    #[test]
    fn a_shared_handle_is_the_provided_one_and_missing_for_a_type_nobody_provided() {
        let clock: Arc<Time> = Arc::new(Fixed(7));
        let name = Arc::new(String::from("fox"));
        let dependencies = Dependencies::default().provide(clock.clone()).provide(name.clone());
        assert!(Arc::ptr_eq(&dependencies.shared::<Time>().unwrap(), &clock));
        assert!(Arc::ptr_eq(&dependencies.shared::<String>().unwrap(), &name));
        assert!(dependencies.shared::<u32>().is_none());
    }

    #[test]
    fn merged_dependencies_keep_what_only_one_side_has_and_take_the_others_where_both_have_one() {
        let left: Arc<Time> = Arc::new(Fixed(1));
        let right: Arc<Time> = Arc::new(Fixed(2));
        let left_only = Dependencies::default().provide(left).provide(Arc::new(String::from("left")));
        let right_only = Dependencies::default().provide(right).provide(Arc::new(7_u32));
        let merged = left_only.clone().merged(&right_only);
        assert_eq!(merged.get::<Time>().map(Clock::now), Some(2), "the other side's");
        assert_eq!(merged.get::<String>().map(String::as_str), Some("left"));
        assert_eq!(merged.get::<u32>(), Some(&7));
        assert_eq!(
            left_only.get::<Time>().map(Clock::now),
            Some(1),
            "the receiver's origin is untouched"
        );
        assert!(right_only.get::<String>().is_none());
    }
}
