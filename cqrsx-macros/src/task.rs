//! `#[derive(Task)]`: the stored type and version of a task's payload, its wire format.
use crate::attribute::{Pairs, not_empty};
use proc_macro2::{Literal, TokenStream};
use quote::quote;
use syn::{DeriveInput, Error};

/// `impl ::cqrsx::task::format::TaskPayload` with the `name` and the `version` of `#[task(name = "…", version = N)]`.
pub(crate) fn expand_task(input: DeriveInput) -> syn::Result<TokenStream> {
    let attribute = Pairs::parse(&input.attrs, "task", &["name", "version"])?;
    let Some(span) = attribute.span else {
        return Err(Error::new_spanned(
            &input.ident,
            "`#[derive(Task)]` needs `#[task(name = \"…\", version = N)]`",
        ));
    };
    let name = attribute
        .string("name")?
        .ok_or_else(|| Error::new(span, "`#[task]` needs `name`"))?;
    let version = attribute
        .integer("version")?
        .ok_or_else(|| Error::new(span, "`#[task]` needs `version`"))?;
    let name = not_empty(name, "the task type must not be empty")?;
    let number: i32 = version.base10_parse()?;
    if number <= 0 {
        return Err(Error::new(version.span(), "the task version must be positive"));
    }
    let number = Literal::i32_unsuffixed(number);
    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cqrsx::task::format::TaskPayload for #ident #type_generics #where_clause {
            const TASK_TYPE: &'static str = #name;
            const TASK_VERSION: i32 = #number;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn refusal(input: DeriveInput) -> String {
        expand_task(input).err().map(|error| error.to_string()).unwrap_or_default()
    }

    #[test]
    fn a_task_gets_the_type_and_the_version_of_its_attribute() {
        let expanded = expand_task(parse_quote! {
            #[derive(Serialize)]
            #[task(name = "encore_record_cancellation", version = 1)]
            pub struct RecordCancellation { pub refund: u32 }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::task::format::TaskPayload for RecordCancellation {
                const TASK_TYPE: &'static str = "encore_record_cancellation";
                const TASK_VERSION: i32 = 1;
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
        let generic = expand_task(parse_quote! {
            #[task(version = 2_000_000_000, name = "greeting")]
            struct Greeting<'a, T> where T: Clone { text: &'a T }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl<'a, T> ::cqrsx::task::format::TaskPayload for Greeting<'a, T> where T: Clone {
                const TASK_TYPE: &'static str = "greeting";
                const TASK_VERSION: i32 = 2000000000;
            }
        };
        assert_eq!(generic.to_string(), expected.to_string());
    }

    #[test]
    fn a_task_without_its_attribute_is_refused() {
        assert_eq!(
            refusal(parse_quote! { #[derive(Serialize)] struct Ship; }),
            "`#[derive(Task)]` needs `#[task(name = \"…\", version = N)]`"
        );
    }

    #[test]
    fn the_task_attribute_needs_a_type_and_a_positive_version() {
        assert_eq!(
            refusal(parse_quote! { #[task(version = 1)] struct Ship; }),
            "`#[task]` needs `name`"
        );
        assert_eq!(
            refusal(parse_quote! { #[task(name = "ship")] struct Ship; }),
            "`#[task]` needs `version`"
        );
        assert_eq!(
            refusal(parse_quote! { #[task(name = "", version = 1)] struct Ship; }),
            "the task type must not be empty"
        );
        assert_eq!(
            refusal(parse_quote! { #[task(name = "ship", version = 0)] struct Ship; }),
            "the task version must be positive"
        );
        assert_eq!(
            refusal(parse_quote! { #[task(name = "ship", version = 3_000_000_000)] struct Ship; }),
            "number too large to fit in target type"
        );
    }

    #[test]
    fn the_task_attribute_refuses_unknown_keys_repeated_keys_and_other_literals() {
        assert_eq!(
            refusal(parse_quote! { #[task(name = "ship", version = 1, policy = "mail")] struct Ship; }),
            "unknown key in `#[task]`: expected `name` or `version`"
        );
        assert_eq!(
            refusal(parse_quote! { #[task(name = "ship", name = "boat", version = 1)] struct Ship; }),
            "`name` is given twice"
        );
        assert_eq!(
            refusal(parse_quote! { #[task(name = "ship", version = "1")] struct Ship; }),
            "`version` takes an integer literal"
        );
        assert_eq!(
            refusal(parse_quote! { #[task(name = 1, version = 1)] struct Ship; }),
            "`name` takes a string literal"
        );
    }
}
