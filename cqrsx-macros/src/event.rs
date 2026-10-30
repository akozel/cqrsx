//! `#[derive(Event)]`: the stored name and version of one event type; `#[derive(Events)]`: an aggregate's enum of
//! events, dispatched to the event type of each variant.
use crate::attribute::{Pairs, not_empty};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields};

/// `impl ::cqrsx::cqrs::event::EventKind` with the `name` and the `version` of `#[event(name = "…", version = "…")]`.
pub(crate) fn expand_event(input: &DeriveInput) -> syn::Result<TokenStream> {
    let attribute = Pairs::parse(&input.attrs, "event", &["name", "version"])?;
    let Some(span) = attribute.span else {
        return Err(Error::new_spanned(
            &input.ident,
            "`#[derive(Event)]` needs `#[event(name = \"…\", version = \"…\")]`",
        ));
    };
    let name = attribute
        .string("name")?
        .ok_or_else(|| Error::new(span, "`#[event]` needs `name`"))?;
    let version = attribute
        .string("version")?
        .ok_or_else(|| Error::new(span, "`#[event]` needs `version`"))?;
    let name = not_empty(name, "the event name must not be empty")?;
    let version = not_empty(version, "the event version must not be empty")?;
    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cqrsx::cqrs::event::EventKind for #ident #type_generics #where_clause {
            const NAME: &'static str = #name;
            const VERSION: &'static str = #version;
        }
    })
}

/// `impl ::cqrsx::cqrs::event::DomainEvent` for an enum whose every variant carries one event type: the name and
/// the version of a value are the `EventKind` of its variant's type.
pub(crate) fn expand_events(input: &DeriveInput) -> syn::Result<TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "`#[derive(Events)]` goes on an enum of event types",
        ));
    };
    let payloads = data
        .variants
        .iter()
        .map(|variant| {
            let mut types = variant.fields.iter().map(|field| &field.ty);
            match (&variant.fields, types.next(), types.next()) {
                (Fields::Unnamed(_), Some(ty), None) => Ok((&variant.ident, ty)),
                _ => Err(Error::new_spanned(
                    variant,
                    "every variant carries one event type: `Variant(EventType)`",
                )),
            }
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let arms = |constant: &str| {
        let constant = format_ident!("{constant}");
        payloads.iter().map(move |(variant, payload)| {
            quote! { Self::#variant(_) => <#payload as ::cqrsx::cqrs::event::EventKind>::#constant, }
        })
    };
    let (names, versions) = (arms("NAME"), arms("VERSION"));
    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cqrsx::cqrs::event::DomainEvent for #ident #type_generics #where_clause {
            fn event_type(&self) -> &'static str {
                match *self { #(#names)* }
            }
            fn event_version(&self) -> &'static str {
                match *self { #(#versions)* }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn refusal(result: syn::Result<TokenStream>) -> String {
        result.err().map(|error| error.to_string()).unwrap_or_default()
    }

    #[test]
    fn an_event_type_gets_the_name_and_the_version_of_its_attribute() {
        let expanded = expand_event(&parse_quote! {
            #[derive(Clone)]
            #[event(name = "BookingCancelled", version = "1.0")]
            pub struct BookingCancelled { pub seat: String }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::cqrs::event::EventKind for BookingCancelled {
                const NAME: &'static str = "BookingCancelled";
                const VERSION: &'static str = "1.0";
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn a_generic_event_type_keeps_its_parameters_and_bounds() {
        let expanded = expand_event(&parse_quote! {
            #[event(version = "2", name = "Wrapped")]
            enum Wrapped<'a, T: Clone> where T: Send { One(&'a T) }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl<'a, T: Clone> ::cqrsx::cqrs::event::EventKind for Wrapped<'a, T> where T: Send {
                const NAME: &'static str = "Wrapped";
                const VERSION: &'static str = "2";
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn an_event_type_without_its_attribute_is_refused() {
        assert_eq!(
            refusal(expand_event(&parse_quote! { #[serde(tag = "type")] struct Booked; })),
            "`#[derive(Event)]` needs `#[event(name = \"…\", version = \"…\")]`"
        );
    }

    #[test]
    fn the_event_attribute_needs_a_name_and_a_version_that_are_not_empty() {
        assert_eq!(
            refusal(expand_event(&parse_quote! { #[event(version = "1.0")] struct Booked; })),
            "`#[event]` needs `name`"
        );
        assert_eq!(
            refusal(expand_event(&parse_quote! { #[event(name = "Booked")] struct Booked; })),
            "`#[event]` needs `version`"
        );
        assert_eq!(
            refusal(expand_event(
                &parse_quote! { #[event(name = "", version = "1.0")] struct Booked; }
            )),
            "the event name must not be empty"
        );
        assert_eq!(
            refusal(expand_event(
                &parse_quote! { #[event(name = "Booked", version = "")] struct Booked; }
            )),
            "the event version must not be empty"
        );
    }

    #[test]
    fn the_event_attribute_refuses_unknown_keys_repeated_keys_and_other_literals() {
        assert_eq!(
            refusal(expand_event(
                &parse_quote! { #[event(name = "Booked", type = "x")] struct Booked; }
            )),
            "unknown key in `#[event]`: expected `name` or `version`"
        );
        assert_eq!(
            refusal(expand_event(
                &parse_quote! { #[event(name = "Booked", version = "1.0", version = "2.0")] struct Booked; }
            )),
            "`version` is given twice"
        );
        assert_eq!(
            refusal(expand_event(
                &parse_quote! { #[event(name = "Booked", version = 1)] struct Booked; }
            )),
            "`version` takes a string literal"
        );
        assert_eq!(
            refusal(expand_event(
                &parse_quote! { #[event(name = 7, version = "1.0")] struct Booked; }
            )),
            "`name` takes a string literal"
        );
    }

    #[test]
    fn an_enum_of_events_takes_the_name_and_the_version_from_each_payload() {
        let expanded = expand_events(&parse_quote! {
            #[serde(tag = "type")]
            pub enum ConcertEvent {
                SeatBooked(SeatBooked),
                BookingCancelled(events::BookingCancelled),
            }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::cqrs::event::DomainEvent for ConcertEvent {
                fn event_type(&self) -> &'static str {
                    match *self {
                        Self::SeatBooked(_) => <SeatBooked as ::cqrsx::cqrs::event::EventKind>::NAME,
                        Self::BookingCancelled(_) =>
                            <events::BookingCancelled as ::cqrsx::cqrs::event::EventKind>::NAME,
                    }
                }
                fn event_version(&self) -> &'static str {
                    match *self {
                        Self::SeatBooked(_) => <SeatBooked as ::cqrsx::cqrs::event::EventKind>::VERSION,
                        Self::BookingCancelled(_) =>
                            <events::BookingCancelled as ::cqrsx::cqrs::event::EventKind>::VERSION,
                    }
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn an_enum_of_events_keeps_its_parameters_and_bounds() {
        let expanded = expand_events(&parse_quote! { enum Either<T: Send> where T: Sync { One(T) } }).unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl<T: Send> ::cqrsx::cqrs::event::DomainEvent for Either<T> where T: Sync {
                fn event_type(&self) -> &'static str {
                    match *self { Self::One(_) => <T as ::cqrsx::cqrs::event::EventKind>::NAME, }
                }
                fn event_version(&self) -> &'static str {
                    match *self { Self::One(_) => <T as ::cqrsx::cqrs::event::EventKind>::VERSION, }
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn an_enum_of_events_without_variants_matches_nothing() {
        let expanded = expand_events(&parse_quote! { enum Never {} }).unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::cqrs::event::DomainEvent for Never {
                fn event_type(&self) -> &'static str { match *self {} }
                fn event_version(&self) -> &'static str { match *self {} }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn every_variant_carries_one_event_type() {
        for input in [
            parse_quote! { enum E { A(A), Cancelled } },
            parse_quote! { enum E { A(A), Cancelled { seat: String } } },
            parse_quote! { enum E { A(A), Cancelled(String, u32) } },
            parse_quote! { enum E { A(A), Cancelled() } },
        ] {
            assert_eq!(
                refusal(expand_events(&input)),
                "every variant carries one event type: `Variant(EventType)`"
            );
        }
    }

    #[test]
    fn events_go_on_an_enum() {
        for input in [
            parse_quote! { struct ConcertEvent(SeatBooked); },
            parse_quote! { union ConcertEvent { seat: u32 } },
        ] {
            assert_eq!(
                refusal(expand_events(&input)),
                "`#[derive(Events)]` goes on an enum of event types"
            );
        }
    }
}
