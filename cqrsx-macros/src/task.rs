//! `#[derive(Task)]`: the stored type and version of a task's payload, its wire format, and the fields it keeps in its
//! metadata.
use crate::attribute::{Pairs, marked, not_empty};
use proc_macro2::{Ident, Literal, TokenStream, TokenTree};
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Error, Field, LitStr, Meta, ext::IdentExt};

/// `impl ::cqrsx::task::Task` with the `name` and the `version` of `#[task(name = "…", version = N)]`
/// and, when the struct has fields `#[label]`, their names in field order as `LABELS`.
pub(crate) fn expand_task(input: &DeriveInput) -> syn::Result<TokenStream> {
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
    let labels = labels(input)?;
    let labels = (!labels.is_empty()).then(|| quote! { const LABELS: &'static [&'static str] = &[#(#labels),*]; });
    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cqrsx::task::Task for #ident #type_generics #where_clause {
            const TYPE: &'static str = #name;
            const VERSION: i32 = #number;
            #labels
        }
    })
}

/// The names of the fields `#[label]`, in field order. A label is a named field of a struct whose key serde keeps:
/// it moves from the body to the metadata under its name, so a serde attribute that renames or drops the key refuses.
fn labels(input: &DeriveInput) -> syn::Result<Vec<LitStr>> {
    let fields: Vec<&Field> = match &input.data {
        Data::Struct(data) => data.fields.iter().collect(),
        Data::Enum(data) => {
            let fields = data.variants.iter().flat_map(|variant| &variant.fields);
            return refuse_labels(
                fields,
                "`#[label]` goes on a field of a struct: a task's labels are its fields",
            );
        }
        Data::Union(data) => {
            return refuse_labels(
                &data.fields.named,
                "`#[label]` goes on a field of a struct: a task's labels are its fields",
            );
        }
    };
    let mut labels = Vec::new();
    for field in fields {
        let Some(attribute) = marked(&field.attrs, "label")? else {
            continue;
        };
        let Some(name) = &field.ident else {
            return Err(Error::new_spanned(attribute, "`#[label]` needs a named field"));
        };
        if let Some(key) = serde_key(&field.attrs, &["rename", "flatten", "skip", "skip_serializing"]) {
            return Err(Error::new(
                key.span(),
                format!("a `#[label]` field moves to the metadata under its name: `#[serde({key})]` changes or drops it"),
            ));
        }
        labels.push(LitStr::new(&name.unraw().to_string(), name.span()));
    }
    if !labels.is_empty()
        && let Some(key) = serde_key(&input.attrs, &["rename_all"])
    {
        return Err(Error::new(
            key.span(),
            format!("the `#[label]` fields move to the metadata under their names: `#[serde({key})]` changes them"),
        ));
    }
    Ok(labels)
}

/// Refuses the first of `fields` marked `#[label]` with `message`.
fn refuse_labels<'a>(fields: impl IntoIterator<Item = &'a Field>, message: &str) -> syn::Result<Vec<LitStr>> {
    for field in fields {
        if let Some(attribute) = marked(&field.attrs, "label")? {
            return Err(Error::new_spanned(attribute, message));
        }
    }
    Ok(Vec::new())
}

/// The first of `keys` that a `#[serde(…)]` attribute of `attributes` gives: a key is an identifier at the top level of
/// the attribute's list (`rename = "…"`, `rename(serialize = "…")`, `flatten`).
fn serde_key(attributes: &[Attribute], keys: &[&str]) -> Option<Ident> {
    attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("serde"))
        .filter_map(|attribute| match &attribute.meta {
            Meta::List(list) => Some(list.tokens.clone()),
            _ => None,
        })
        .flatten()
        .find_map(|tree| match tree {
            TokenTree::Ident(ident) if keys.iter().any(|key| ident == key) => Some(ident),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn refusal(input: &DeriveInput) -> String {
        expand_task(input).err().map(|error| error.to_string()).unwrap_or_default()
    }

    #[test]
    fn a_task_gets_the_type_and_the_version_of_its_attribute() {
        let expanded = expand_task(&parse_quote! {
            #[derive(Serialize)]
            #[task(name = "encore_record_cancellation", version = 1)]
            pub struct RecordCancellation { pub refund: u32 }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::task::Task for RecordCancellation {
                const TYPE: &'static str = "encore_record_cancellation";
                const VERSION: i32 = 1;
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
        let generic = expand_task(&parse_quote! {
            #[task(version = 2_000_000_000, name = "greeting")]
            struct Greeting<'a, T> where T: Clone { text: &'a T }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl<'a, T> ::cqrsx::task::Task for Greeting<'a, T> where T: Clone {
                const TYPE: &'static str = "greeting";
                const VERSION: i32 = 2000000000;
            }
        };
        assert_eq!(generic.to_string(), expected.to_string());
    }

    #[test]
    fn labelled_fields_are_the_labels_of_the_task_in_field_order() {
        let expanded = expand_task(&parse_quote! {
            #[derive(Serialize)]
            #[serde(deny_unknown_fields)]
            #[task(name = "issue_refund", version = 1)]
            pub struct IssueRefund {
                pub cancellation_id: Uuid,
                #[label]
                #[serde(default)]
                pub tenant: TenantId,
                pub amount: u32,
                #[label]
                pub r#type: Kind,
            }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::task::Task for IssueRefund {
                const TYPE: &'static str = "issue_refund";
                const VERSION: i32 = 1;
                const LABELS: &'static [&'static str] = &["tenant", "type"];
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
        let renamed_without_labels = expand_task(&parse_quote! {
            #[serde(rename_all = "camelCase")]
            #[task(name = "ship", version = 1)]
            struct Ship { #[serde(rename = "to")] address: String }
        })
        .unwrap();
        assert!(
            !renamed_without_labels.to_string().contains("LABELS"),
            "{renamed_without_labels}"
        );
    }

    #[test]
    fn a_label_is_refused_with_arguments_twice_without_a_name_off_a_struct_and_where_serde_renames_or_drops_it() {
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 1)] struct Ship { #[label(key = "x")] to: String } }),
            "`#[label]` takes no arguments"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 1)] struct Ship { #[label] #[label] to: String } }),
            "`#[label]` is given twice"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 1)] struct Ship(#[label] String); }),
            "`#[label]` needs a named field"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 1)] enum Ship { By { #[label] to: String } } }),
            "`#[label]` goes on a field of a struct: a task's labels are its fields"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 1)] union Ship { #[label] to: u32 } }),
            "`#[label]` goes on a field of a struct: a task's labels are its fields"
        );
        assert!(expand_task(&parse_quote! { #[task(name = "ship", version = 1)] enum Ship { By { to: String } } }).is_ok());
        assert!(expand_task(&parse_quote! { #[task(name = "ship", version = 1)] union Ship { to: u32 } }).is_ok());
        assert!(
            expand_task(&parse_quote! { #[task(name = "ship", version = 1)] struct Ship { #[label] #[serde] to: String } })
                .is_ok(),
            "a serde attribute without a list names no key"
        );
        for (attribute, key) in [
            (quote! { #[serde(rename = "destination")] }, "rename"),
            (quote! { #[serde(rename(serialize = "destination"))] }, "rename"),
            (quote! { #[serde(default, flatten)] }, "flatten"),
            (quote! { #[serde(skip)] }, "skip"),
            (quote! { #[serde(skip_serializing)] }, "skip_serializing"),
        ] {
            let input: DeriveInput = parse_quote! {
                #[task(name = "ship", version = 1)]
                struct Ship { #[label] #attribute to: String }
            };
            assert_eq!(
                refusal(&input),
                format!("a `#[label]` field moves to the metadata under its name: `#[serde({key})]` changes or drops it")
            );
        }
        assert_eq!(
            refusal(&parse_quote! {
                #[serde(rename_all = "camelCase")]
                #[task(name = "ship", version = 1)]
                struct Ship { #[label] to: String }
            }),
            "the `#[label]` fields move to the metadata under their names: `#[serde(rename_all)]` changes them"
        );
    }

    #[test]
    fn a_task_without_its_attribute_is_refused() {
        assert_eq!(
            refusal(&parse_quote! { #[derive(Serialize)] struct Ship; }),
            "`#[derive(Task)]` needs `#[task(name = \"…\", version = N)]`"
        );
    }

    #[test]
    fn the_task_attribute_needs_a_type_and_a_positive_version() {
        assert_eq!(
            refusal(&parse_quote! { #[task(version = 1)] struct Ship; }),
            "`#[task]` needs `name`"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship")] struct Ship; }),
            "`#[task]` needs `version`"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "", version = 1)] struct Ship; }),
            "the task type must not be empty"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 0)] struct Ship; }),
            "the task version must be positive"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 3_000_000_000)] struct Ship; }),
            "number too large to fit in target type"
        );
    }

    #[test]
    fn the_task_attribute_refuses_unknown_keys_repeated_keys_and_other_literals() {
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = 1, policy = "mail")] struct Ship; }),
            "unknown key in `#[task]`: expected `name` or `version`"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", name = "boat", version = 1)] struct Ship; }),
            "`name` is given twice"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = "ship", version = "1")] struct Ship; }),
            "`version` takes an integer literal"
        );
        assert_eq!(
            refusal(&parse_quote! { #[task(name = 1, version = 1)] struct Ship; }),
            "`name` takes a string literal"
        );
    }
}
