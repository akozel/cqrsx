//! Derive macros of cqrsx. Use them through cqrsx, which re-exports them as `cqrsx::{Command, Commands, Event, Events,
//! Task, Rejection}`: the expansions name `::cqrsx` paths only.
//!
//! Every entry point is one call of its expansion, a function over `proc_macro2` that the unit tests call directly;
//! what does not compile is shown by the `compile_fail` examples of cqrsx.
mod attribute;
mod command;
mod event;
mod rejection;
mod task;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::DeriveInput;

/// What an observer knows of one command besides its decision: on a struct, implements
/// `cqrsx::cqrs::command::CommandInfo`. Its name is the type's (`CancelBooking`); the field marked `#[command_id]`, a
/// `Uuid` and at most one, is the command's id; the fields marked `#[label]` are its labels, in field order, each as
/// its name and its `Display`. An enum, a second `#[command_id]`, an attribute with arguments or on a field without a
/// name, and a command id that is not a `Uuid` do not compile. Examples: `cqrsx::cqrs::command::CommandInfo`.
#[proc_macro_derive(Command, attributes(command_id, label))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    run(command::expand_command, input.into()).into()
}

/// An aggregate's enum of commands, every variant carrying one command type (`CancelBooking(CancelBooking)`):
/// implements `cqrsx::cqrs::command::decide::Decide<A>` for every aggregate `A` that each variant's type decides, by
/// handing the command to that type's `decide`, and `cqrsx::cqrs::command::CommandInfo` by the same delegation. The
/// derive does not know the aggregate: `ConcertCommand: Decide<Concert>` holds once every variant decides `Concert`.
/// A variant without exactly one field does not compile, nor does a variant whose type does not decide the
/// aggregate the enum is the command of. Examples: `cqrsx::cqrs::command::decide::Decide`.
#[proc_macro_derive(Commands)]
pub fn derive_commands(input: TokenStream) -> TokenStream {
    run(command::expand_commands, input.into()).into()
}

/// The stored name and version of one event type: `#[event(name = "BookingCancelled", version = "1.0")]` on the
/// type implements `cqrsx::cqrs::event::EventKind` with them. Both are stored with every event of the type and
/// never change. A missing attribute or key, another key, a key given twice, a value that is not a string and an
/// empty value do not compile. Examples: `cqrsx::cqrs::event::EventKind`.
#[proc_macro_derive(Event, attributes(event))]
pub fn derive_event(input: TokenStream) -> TokenStream {
    run(event::expand_event, input.into()).into()
}

/// An aggregate's enum of events, every variant carrying one event type (`SeatBooked(SeatBooked)`): implements
/// `cqrsx::cqrs::event::DomainEvent`, whose `event_type` and `event_version` are the `EventKind` of the variant's
/// type. A variant without exactly one field, or whose type has no `#[derive(Event)]`, does not compile; other
/// attributes of the enum (`#[serde(tag = "type")]`) stay as they are. Examples: `cqrsx::cqrs::event::EventKind`.
#[proc_macro_derive(Events)]
pub fn derive_events(input: TokenStream) -> TokenStream {
    run(event::expand_events, input.into()).into()
}

/// The wire format of a task's payload: `#[task(name = "encore_record_cancellation", version = 1)]` on the type
/// implements `cqrsx::task::format::TaskPayload` with them (`TASK_TYPE`, `TASK_VERSION`). A queued task keeps them, so
/// they never change. A missing attribute or key, another key, a key given twice, a type that is not a string or is
/// empty and a version that is not a positive `i32` do not compile. Examples: `cqrsx::task::format::TaskPayload`.
#[proc_macro_derive(Task, attributes(task))]
pub fn derive_task(input: TokenStream) -> TokenStream {
    run(task::expand_task, input.into()).into()
}

/// A business refusal as the API and the activity feed report it, on an enum of rejections: implements
/// `cqrsx::cqrs::rejection::Rejection`. The code of a variant is its name in snake case (`SeatTaken` → `seat_taken`),
/// or what `#[rejection(code = "…")]` on the variant gives; its params are its named fields as a JSON object, a unit
/// variant's are `{}`. A tuple variant, the attribute on the enum, another key, an empty code and two variants with
/// one code do not compile. Examples: `cqrsx::cqrs::rejection::Rejection`.
#[proc_macro_derive(Rejection, attributes(rejection))]
pub fn derive_rejection(input: TokenStream) -> TokenStream {
    run(rejection::expand_rejection, input.into()).into()
}

/// Parses a derive input and expands it; an input that does not parse and a refused expansion become a
/// `compile_error!` at their place.
fn run(expand: fn(DeriveInput) -> syn::Result<TokenStream2>, input: TokenStream2) -> TokenStream2 {
    syn::parse2(input)
        .and_then(expand)
        .unwrap_or_else(syn::Error::into_compile_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    fn named(input: DeriveInput) -> syn::Result<TokenStream2> {
        let ident = input.ident;
        Ok(quote! { const _: &str = stringify!(#ident); })
    }

    fn refused(input: DeriveInput) -> syn::Result<TokenStream2> {
        Err(syn::Error::new_spanned(input.ident, "refused"))
    }

    #[test]
    fn run_expands_a_parsed_input() {
        assert_eq!(
            run(named, quote! { struct Seat; }).to_string(),
            quote! { const _: &str = stringify!(Seat); }.to_string()
        );
    }

    #[test]
    fn run_reports_an_unparsable_input_and_an_expansion_error_as_compile_errors() {
        assert_eq!(
            run(refused, quote! { struct Seat; }).to_string(),
            quote! { ::core::compile_error! { "refused" } }.to_string()
        );
        let unparsable = run(named, quote! { fn seat() {} }).to_string();
        assert!(unparsable.starts_with(":: core :: compile_error !"), "{unparsable}");
    }
}
