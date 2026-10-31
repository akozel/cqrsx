//! `#[derive(Rejection)]`: a business refusal as the API and the activity feed report it, its code and its params.
use crate::attribute::{Pairs, not_empty};
use heck::ToSnakeCase;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, Ident, LitStr, ext::IdentExt};

/// `impl ::cqrsx::domain::rejection::Rejection` for an enum of rejections: the code of a variant is its name in snake
/// case unless `#[rejection(code = "…")]` gives it, its params are its named fields as a JSON object (a unit variant
/// has `{}`).
pub(crate) fn expand_rejection(input: &DeriveInput) -> syn::Result<TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "`#[derive(Rejection)]` goes on an enum of rejections",
        ));
    };
    if let Some(attribute) = input.attrs.iter().find(|attribute| attribute.path().is_ident("rejection")) {
        return Err(Error::new_spanned(attribute, "`#[rejection]` goes on a variant"));
    }
    let mut codes: Vec<(String, &Ident)> = Vec::new();
    let (mut code_arms, mut params_arms) = (Vec::new(), Vec::new());
    for variant in &data.variants {
        let ident = &variant.ident;
        let code = match Pairs::parse(&variant.attrs, "rejection", &["code"])?.string("code")? {
            Some(code) => not_empty(code, "a rejection code must not be empty")?.value(),
            None => ident.unraw().to_string().to_snake_case(),
        };
        if let Some((_, first)) = codes.iter().find(|(given, _)| *given == code) {
            return Err(Error::new_spanned(
                ident,
                format!("the code `{code}` is reported by `{first}` already"),
            ));
        }
        let params = match &variant.fields {
            Fields::Unit => quote! { Self::#ident => ::cqrsx::__private::serde_json::json!({}), },
            Fields::Named(fields) => {
                let names: Vec<&Ident> = fields.named.iter().filter_map(|field| field.ident.as_ref()).collect();
                let keys = names.iter().map(|name| LitStr::new(&name.unraw().to_string(), name.span()));
                quote! {
                    Self::#ident { #(ref #names),* } => ::cqrsx::__private::serde_json::json!({ #(#keys: #names),* }),
                }
            }
            Fields::Unnamed(_) => {
                return Err(Error::new_spanned(
                    variant,
                    "`#[derive(Rejection)]` reports named fields as params: a tuple variant has no names",
                ));
            }
        };
        let literal = LitStr::new(&code, ident.span());
        code_arms.push(quote! { Self::#ident { .. } => #literal, });
        params_arms.push(params);
        codes.push((code, ident));
    }
    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cqrsx::domain::rejection::Rejection for #ident #type_generics #where_clause {
            fn code(&self) -> &'static str {
                match *self { #(#code_arms)* }
            }
            fn params(&self) -> ::cqrsx::__private::serde_json::Value {
                match *self { #(#params_arms)* }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn refusal(input: &DeriveInput) -> String {
        expand_rejection(input)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default()
    }

    #[test]
    fn a_rejection_reports_the_variant_in_snake_case_and_its_named_fields() {
        let expanded = expand_rejection(&parse_quote! {
            #[derive(Debug, thiserror::Error)]
            pub enum ConcertError {
                #[error("this concert has not been announced")]
                ConcertNotFound,
                #[error("seat {seat} is already taken by {by}")]
                SeatTaken { seat: SeatId, by: String },
                Empty {},
            }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::domain::rejection::Rejection for ConcertError {
                fn code(&self) -> &'static str {
                    match *self {
                        Self::ConcertNotFound { .. } => "concert_not_found",
                        Self::SeatTaken { .. } => "seat_taken",
                        Self::Empty { .. } => "empty",
                    }
                }
                fn params(&self) -> ::cqrsx::__private::serde_json::Value {
                    match *self {
                        Self::ConcertNotFound => ::cqrsx::__private::serde_json::json!({}),
                        Self::SeatTaken { ref seat, ref by } =>
                            ::cqrsx::__private::serde_json::json!({ "seat": seat, "by": by }),
                        Self::Empty {} => ::cqrsx::__private::serde_json::json!({}),
                    }
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn a_generic_rejection_keeps_its_parameters_and_bounds() {
        let expanded = expand_rejection(&parse_quote! { enum Refused<T: Clone> where T: Send { Late { by: T } } }).unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl<T: Clone> ::cqrsx::domain::rejection::Rejection for Refused<T> where T: Send {
                fn code(&self) -> &'static str {
                    match *self { Self::Late { .. } => "late", }
                }
                fn params(&self) -> ::cqrsx::__private::serde_json::Value {
                    match *self { Self::Late { ref by } => ::cqrsx::__private::serde_json::json!({ "by": by }), }
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn a_code_attribute_overrides_the_variant_name() {
        let expanded = expand_rejection(&parse_quote! {
            enum ConcertError {
                #[rejection(code = "r1_not_announced")]
                ConcertNotFound,
            }
        })
        .unwrap()
        .to_string();
        assert!(
            expanded.contains(&quote! { Self::ConcertNotFound { .. } => "r1_not_announced", }.to_string()),
            "{expanded}"
        );
        assert!(!expanded.contains("concert_not_found"), "{expanded}");
    }

    #[test]
    fn a_raw_field_name_is_reported_without_its_prefix() {
        let expanded = expand_rejection(&parse_quote! { enum ConcertError { Wrong { r#type: String } } })
            .unwrap()
            .to_string();
        assert!(
            expanded.contains(
                &quote! { Self::Wrong { ref r#type } => ::cqrsx::__private::serde_json::json!({ "type": r#type }), }.to_string()
            ),
            "{expanded}"
        );
    }

    #[test]
    fn an_enum_of_rejections_without_variants_matches_nothing() {
        let expanded = expand_rejection(&parse_quote! { enum Never {} }).unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::domain::rejection::Rejection for Never {
                fn code(&self) -> &'static str { match *self {} }
                fn params(&self) -> ::cqrsx::__private::serde_json::Value { match *self {} }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn a_tuple_variant_is_refused() {
        assert_eq!(
            refusal(&parse_quote! { enum ConcertError { SoldOut, SeatTaken(SeatId, String) } }),
            "`#[derive(Rejection)]` reports named fields as params: a tuple variant has no names"
        );
    }

    #[test]
    fn rejections_go_on_an_enum() {
        for input in [
            parse_quote! { struct TicketLimit { limit: u32 } },
            parse_quote! { union TicketLimit { limit: u32 } },
        ] {
            assert_eq!(refusal(&input), "`#[derive(Rejection)]` goes on an enum of rejections");
        }
    }

    #[test]
    fn the_rejection_attribute_goes_on_a_variant_and_takes_a_code() {
        assert_eq!(
            refusal(&parse_quote! { #[rejection(code = "x")] enum ConcertError { SoldOut } }),
            "`#[rejection]` goes on a variant"
        );
        assert_eq!(
            refusal(&parse_quote! { enum ConcertError { #[rejection(name = "x")] SoldOut } }),
            "unknown key in `#[rejection]`: expected `code`"
        );
        assert_eq!(
            refusal(&parse_quote! { enum ConcertError { #[rejection(code = "")] SoldOut } }),
            "a rejection code must not be empty"
        );
        assert_eq!(
            refusal(&parse_quote! { enum ConcertError { #[rejection(code = "x", code = "y")] SoldOut } }),
            "`code` is given twice"
        );
        assert_eq!(
            refusal(&parse_quote! { enum ConcertError { #[rejection(code = 1)] SoldOut } }),
            "`code` takes a string literal"
        );
    }

    #[test]
    fn two_variants_with_one_code_are_refused() {
        assert_eq!(
            refusal(&parse_quote! { enum ConcertError { SoldOut, #[rejection(code = "sold_out")] NoSeatsLeft } }),
            "the code `sold_out` is reported by `SoldOut` already"
        );
    }

    /// The codes of rules R1–R11 are the names of their variants in snake case (the API contract, G-35); an acronym
    /// is one word.
    #[test]
    fn snake_case_splits_the_words_of_a_variant() {
        let codes: Vec<String> = [
            "ConcertNotFound",
            "AlreadyAnnounced",
            "UnknownSeat",
            "SoldOut",
            "SeatTaken",
            "TicketLimit",
            "SeatNotBooked",
            "NotYourTicket",
            "AlreadyRecorded",
            "UnknownTicket",
            "AlreadyCancelled",
            "HTTPTimeout",
        ]
        .iter()
        .map(|name| name.to_snake_case())
        .collect();
        assert_eq!(
            codes,
            [
                "concert_not_found",
                "already_announced",
                "unknown_seat",
                "sold_out",
                "seat_taken",
                "ticket_limit",
                "seat_not_booked",
                "not_your_ticket",
                "already_recorded",
                "unknown_ticket",
                "already_cancelled",
                "http_timeout",
            ]
        );
    }
}
