//! The `#[name(key = literal, …)]` attributes of the derives.
use proc_macro2::Span;
use syn::{Attribute, Error, Lit, LitInt, LitStr, Meta, spanned::Spanned};

/// The `key = literal` pairs of every `#[name(…)]` attribute of an item, in their order.
pub(crate) struct Pairs {
    /// Where the first such attribute is; `None` when the item has none.
    pub(crate) span: Option<Span>,
    pairs: Vec<(String, Lit)>,
}

impl Pairs {
    /// Reads the `#[name(…)]` attributes of `attributes`. A key outside `keys`, a key given twice and a value that is
    /// not a literal are errors at their place.
    pub(crate) fn parse(attributes: &[Attribute], name: &str, keys: &[&str]) -> syn::Result<Self> {
        let mut found = Self {
            span: None,
            pairs: Vec::new(),
        };
        for attribute in attributes.iter().filter(|attribute| attribute.path().is_ident(name)) {
            found.span.get_or_insert_with(|| attribute.span());
            attribute.parse_nested_meta(|meta| {
                let key = meta.path.get_ident().map(ToString::to_string).unwrap_or_default();
                if !keys.contains(&key.as_str()) {
                    let expected: Vec<String> = keys.iter().map(|key| format!("`{key}`")).collect();
                    return Err(meta.error(format!("unknown key in `#[{name}]`: expected {}", expected.join(" or "))));
                }
                if found.pairs.iter().any(|(given, _)| *given == key) {
                    return Err(meta.error(format!("`{key}` is given twice")));
                }
                let value: Lit = meta.value()?.parse()?;
                found.pairs.push((key, value));
                Ok(())
            })?;
        }
        Ok(found)
    }

    fn get(&self, key: &str) -> Option<&Lit> {
        self.pairs.iter().find(|(given, _)| given == key).map(|(_, value)| value)
    }

    /// The string literal of `key`, `None` when the key is not given.
    pub(crate) fn string(&self, key: &str) -> syn::Result<Option<LitStr>> {
        match self.get(key) {
            None => Ok(None),
            Some(Lit::Str(value)) => Ok(Some(value.clone())),
            Some(other) => Err(Error::new(other.span(), format!("`{key}` takes a string literal"))),
        }
    }

    /// The integer literal of `key`, `None` when the key is not given.
    pub(crate) fn integer(&self, key: &str) -> syn::Result<Option<LitInt>> {
        match self.get(key) {
            None => Ok(None),
            Some(Lit::Int(value)) => Ok(Some(value.clone())),
            Some(other) => Err(Error::new(other.span(), format!("`{key}` takes an integer literal"))),
        }
    }
}

/// `value`, refused with `message` when it is empty.
pub(crate) fn not_empty(value: LitStr, message: &str) -> syn::Result<LitStr> {
    if value.value().is_empty() {
        return Err(Error::new(value.span(), message));
    }
    Ok(value)
}

/// The attribute `#[name]` of a field, `None` when it has none; one with arguments or given twice is refused.
pub(crate) fn marked<'a>(attributes: &'a [Attribute], name: &str) -> syn::Result<Option<&'a Attribute>> {
    let mut found = None;
    for attribute in attributes.iter().filter(|attribute| attribute.path().is_ident(name)) {
        if !matches!(attribute.meta, Meta::Path(_)) {
            return Err(Error::new_spanned(attribute, format!("`#[{name}]` takes no arguments")));
        }
        if found.is_some() {
            return Err(Error::new_spanned(attribute, format!("`#[{name}]` is given twice")));
        }
        found = Some(attribute);
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::{DeriveInput, parse_quote};

    fn pairs(input: DeriveInput) -> syn::Result<Pairs> {
        Pairs::parse(&input.attrs, "event", &["name", "version"])
    }

    #[test]
    fn pairs_gather_the_keys_of_every_attribute_of_their_name() {
        let found = pairs(parse_quote! {
            #[serde(rename = "x")]
            #[event(name = "Booked")]
            #[event(version = 2)]
            struct Booked;
        })
        .unwrap();
        assert!(found.span.is_some());
        assert_eq!(
            found.string("name").unwrap().map(|name| name.value()),
            Some("Booked".to_owned())
        );
        assert_eq!(
            found.integer("version").unwrap().map(|v| v.base10_digits().to_owned()),
            Some("2".to_owned())
        );
        assert_eq!(
            found.string("version").err().map(|error| error.to_string()),
            Some("`version` takes a string literal".to_owned())
        );
        assert_eq!(
            found.integer("name").err().map(|error| error.to_string()),
            Some("`name` takes an integer literal".to_owned())
        );
        let none = pairs(parse_quote! { #[serde(rename = "x")] struct Booked; }).unwrap();
        assert!(none.span.is_none());
        assert!(none.string("name").unwrap().is_none());
        assert!(none.integer("version").unwrap().is_none());
    }

    #[test]
    fn pairs_refuse_unknown_keys_repeated_keys_and_values_that_are_not_literals() {
        let error = |input: DeriveInput| pairs(input).err().map(|error| error.to_string());
        assert_eq!(
            error(parse_quote! { #[event(title = "Booked")] struct Booked; }),
            Some("unknown key in `#[event]`: expected `name` or `version`".to_owned())
        );
        assert_eq!(
            error(parse_quote! { #[event(name = "A", name = "B")] struct Booked; }),
            Some("`name` is given twice".to_owned())
        );
        assert_eq!(
            error(parse_quote! { #[event(name = "A")] #[event(name = "B")] struct Booked; }),
            Some("`name` is given twice".to_owned())
        );
        assert_eq!(
            error(parse_quote! { #[event(crate::name = "A")] struct Booked; }),
            Some("unknown key in `#[event]`: expected `name` or `version`".to_owned())
        );
        assert!(error(parse_quote! { #[event(name = BOOKED)] struct Booked; }).is_some());
        assert!(error(parse_quote! { #[event] struct Booked; }).is_some());
    }

    #[test]
    fn an_empty_value_is_refused_with_its_message() {
        assert_eq!(not_empty(parse_quote!("A"), "empty").unwrap().value(), "A");
        assert_eq!(
            not_empty(parse_quote!(""), "empty").err().map(|error| error.to_string()),
            Some("empty".to_owned())
        );
    }
}
