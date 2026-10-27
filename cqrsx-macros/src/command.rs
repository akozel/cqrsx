//! `#[derive(Command)]`: what an observer knows of one command besides its decision; `#[derive(Commands)]`: an
//! aggregate's enum of commands, routed to the decision of each variant's type.
use crate::attribute::marked;
use proc_macro2::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::{Data, DeriveInput, Error, Fields, Ident, LitStr, Type, ext::IdentExt, parse_quote, spanned::Spanned};

/// `impl ::cqrsx::cqrs::command::CommandInfo` for a struct: the type's name, the field `#[command_id]` (at most one) and
/// the fields `#[label]`, in field order, each as its name and its `Display`.
pub(crate) fn expand_command(input: DeriveInput) -> syn::Result<TokenStream> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "`#[derive(Command)]` goes on a struct: an enum of commands derives `Commands`",
        ));
    };
    let mut command_id: Option<(&Ident, &Type)> = None;
    let mut labels = Vec::new();
    for field in &data.fields {
        let id = marked(&field.attrs, "command_id")?;
        let label = marked(&field.attrs, "label")?;
        let Some(attribute) = id.or(label) else {
            continue;
        };
        let Some(name) = &field.ident else {
            return Err(Error::new_spanned(
                attribute,
                "`#[command_id]` and `#[label]` need a named field",
            ));
        };
        if let Some(attribute) = id {
            if command_id.is_some() {
                return Err(Error::new_spanned(attribute, "only one field is the command id"));
            }
            command_id = Some((name, &field.ty));
        }
        if label.is_some() {
            labels.push((LitStr::new(&name.unraw().to_string(), name.span()), name));
        }
    }
    let command_id = match command_id {
        // Spanned at the field's type: a field that is not a `Uuid` is reported there.
        Some((field, ty)) => quote_spanned! { ty.span()=> ::core::option::Option::Some(self.#field) },
        None => quote! { ::core::option::Option::None },
    };
    let labels = labels
        .iter()
        .map(|(key, field)| quote! { (#key, ::std::string::ToString::to_string(&self.#field)) });
    let ident = &input.ident;
    let name = LitStr::new(&ident.unraw().to_string(), ident.span());
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cqrsx::cqrs::command::CommandInfo for #ident #type_generics #where_clause {
            fn name(&self) -> &'static str {
                #name
            }
            fn command_id(&self) -> ::core::option::Option<::cqrsx::__private::Uuid> {
                #command_id
            }
            fn labels(&self) -> ::std::vec::Vec<(&'static str, ::std::string::String)> {
                ::std::vec![#(#labels),*]
            }
        }
    })
}

/// For an enum whose every variant carries one command type: `impl<A> ::cqrsx::cqrs::command::decide::Decide<A>` where
/// every variant's type decides `A` (a `match` that hands each variant to its type's `decide`), and
/// `impl ::cqrsx::cqrs::command::CommandInfo` by the same delegation, where every variant's type has it.
pub(crate) fn expand_commands(input: DeriveInput) -> syn::Result<TokenStream> {
    let Data::Enum(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            "`#[derive(Commands)]` goes on an enum of command types: a struct command derives `Command`",
        ));
    };
    let variants = data
        .variants
        .iter()
        .map(|variant| match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => Ok((&variant.ident, &fields.unnamed[0].ty)),
            _ => Err(Error::new_spanned(
                variant,
                "every variant carries one command type: `Variant(CommandType)`",
            )),
        })
        .collect::<syn::Result<Vec<(&Ident, &Type)>>>()?;
    let ident = &input.ident;
    let aggregate = format_ident!("__A");
    let mut generics = input.generics.clone();
    generics.params.push(parse_quote!(#aggregate));
    let predicates = &mut generics.make_where_clause().predicates;
    predicates.push(parse_quote!(#aggregate: ::cqrsx::cqrs::aggregate::Aggregate));
    for (_, command) in &variants {
        predicates.push(parse_quote!(#command: ::cqrsx::cqrs::command::decide::Decide<#aggregate>));
    }
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let mut info = input.generics.clone();
    let predicates = &mut info.make_where_clause().predicates;
    for (_, command) in &variants {
        predicates.push(parse_quote!(#command: ::cqrsx::cqrs::command::CommandInfo));
    }
    let (info_generics, _, info_where) = info.split_for_impl();
    let (_, type_generics, _) = input.generics.split_for_impl();
    let decide = if variants.is_empty() {
        quote! {
            let _ = (state, rules);
            match self {}
        }
    } else {
        let arms = variants.iter().map(|(variant, _)| {
            quote! {
                Self::#variant(command) => ::cqrsx::cqrs::command::decide::Decide::<#aggregate>::decide(command, state, rules),
            }
        });
        quote! { match self { #(#arms)* } }
    };
    let delegate = |method: &str| {
        let method = format_ident!("{method}");
        let arms = variants.iter().map(move |(variant, _)| {
            quote! { Self::#variant(ref command) => ::cqrsx::cqrs::command::CommandInfo::#method(command), }
        });
        quote! { match *self { #(#arms)* } }
    };
    let (name, command_id, labels) = (delegate("name"), delegate("command_id"), delegate("labels"));
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::cqrsx::cqrs::command::decide::Decide<#aggregate> for #ident #type_generics #where_clause {
            fn decide(
                self,
                state: &#aggregate,
                rules: &<#aggregate as ::cqrsx::cqrs::aggregate::Aggregate>::Rules,
            ) -> ::core::result::Result<
                <#aggregate as ::cqrsx::cqrs::aggregate::Aggregate>::Event,
                <#aggregate as ::cqrsx::cqrs::aggregate::Aggregate>::Error,
            > {
                #decide
            }
        }
        #[automatically_derived]
        impl #info_generics ::cqrsx::cqrs::command::CommandInfo for #ident #type_generics #info_where {
            fn name(&self) -> &'static str {
                #name
            }
            fn command_id(&self) -> ::core::option::Option<::cqrsx::__private::Uuid> {
                #command_id
            }
            fn labels(&self) -> ::std::vec::Vec<(&'static str, ::std::string::String)> {
                #labels
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
    fn a_command_reports_its_name_its_id_field_and_its_labels_in_field_order() {
        let expanded = expand_command(parse_quote! {
            #[derive(Clone)]
            pub struct CancelBooking {
                pub seat: String,
                #[label]
                pub fan_id: FanId,
                #[command_id]
                pub cancellation_id: Uuid,
                #[label]
                pub r#type: Kind,
            }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl ::cqrsx::cqrs::command::CommandInfo for CancelBooking {
                fn name(&self) -> &'static str {
                    "CancelBooking"
                }
                fn command_id(&self) -> ::core::option::Option<::cqrsx::__private::Uuid> {
                    ::core::option::Option::Some(self.cancellation_id)
                }
                fn labels(&self) -> ::std::vec::Vec<(&'static str, ::std::string::String)> {
                    ::std::vec![
                        ("fan_id", ::std::string::ToString::to_string(&self.fan_id)),
                        ("type", ::std::string::ToString::to_string(&self.r#type))
                    ]
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn a_command_without_attributes_has_no_id_and_no_labels_and_a_generic_one_keeps_its_parameters() {
        let expanded = expand_command(parse_quote! { struct r#Ping; }).unwrap().to_string();
        assert!(expanded.contains("\"Ping\""), "{expanded}");
        assert!(expanded.contains(":: core :: option :: Option :: None"), "{expanded}");
        assert!(expanded.contains(":: std :: vec ! []"), "{expanded}");
        let generic = expand_command(parse_quote! { struct Wrap<'a, T: Clone>(&'a T) where T: Send; }).unwrap();
        let expected = quote! {
            impl<'a, T: Clone> ::cqrsx::cqrs::command::CommandInfo for Wrap<'a, T> where T: Send
        };
        assert!(generic.to_string().contains(&expected.to_string()), "{generic}");
    }

    #[test]
    fn the_command_attributes_are_refused_where_they_mean_nothing() {
        assert_eq!(
            refusal(expand_command(parse_quote! { enum Command { A } })),
            "`#[derive(Command)]` goes on a struct: an enum of commands derives `Commands`"
        );
        assert_eq!(
            refusal(expand_command(
                parse_quote! { struct Book { #[command_id] booking_id: Uuid, #[command_id] other: Uuid } }
            )),
            "only one field is the command id"
        );
        assert_eq!(
            refusal(expand_command(
                parse_quote! { struct Book { #[command_id] #[command_id] booking_id: Uuid } }
            )),
            "`#[command_id]` is given twice"
        );
        assert_eq!(
            refusal(expand_command(
                parse_quote! { struct Book { #[label(key = "fan")] fan_id: FanId } }
            )),
            "`#[label]` takes no arguments"
        );
        assert_eq!(
            refusal(expand_command(
                parse_quote! { struct Book { #[command_id = 1] booking_id: Uuid } }
            )),
            "`#[command_id]` takes no arguments"
        );
        assert_eq!(
            refusal(expand_command(parse_quote! { struct Book(#[label] FanId); })),
            "`#[command_id]` and `#[label]` need a named field"
        );
    }

    #[test]
    fn an_enum_of_commands_routes_every_variant_to_its_decision_and_its_info() {
        let expanded = expand_commands(parse_quote! {
            #[derive(Clone)]
            pub enum ConcertCommand {
                BookSeat(BookSeat),
                CancelBooking(commands::CancelBooking),
            }
        })
        .unwrap();
        let expected = quote! {
            #[automatically_derived]
            impl<__A> ::cqrsx::cqrs::command::decide::Decide<__A> for ConcertCommand
            where
                __A: ::cqrsx::cqrs::aggregate::Aggregate,
                BookSeat: ::cqrsx::cqrs::command::decide::Decide<__A>,
                commands::CancelBooking: ::cqrsx::cqrs::command::decide::Decide<__A>
            {
                fn decide(
                    self,
                    state: &__A,
                    rules: &<__A as ::cqrsx::cqrs::aggregate::Aggregate>::Rules,
                ) -> ::core::result::Result<
                    <__A as ::cqrsx::cqrs::aggregate::Aggregate>::Event,
                    <__A as ::cqrsx::cqrs::aggregate::Aggregate>::Error,
                > {
                    match self {
                        Self::BookSeat(command) => ::cqrsx::cqrs::command::decide::Decide::<__A>::decide(command, state, rules),
                        Self::CancelBooking(command) => ::cqrsx::cqrs::command::decide::Decide::<__A>::decide(command, state, rules),
                    }
                }
            }
            #[automatically_derived]
            impl ::cqrsx::cqrs::command::CommandInfo for ConcertCommand
            where
                BookSeat: ::cqrsx::cqrs::command::CommandInfo,
                commands::CancelBooking: ::cqrsx::cqrs::command::CommandInfo
            {
                fn name(&self) -> &'static str {
                    match *self {
                        Self::BookSeat(ref command) => ::cqrsx::cqrs::command::CommandInfo::name(command),
                        Self::CancelBooking(ref command) => ::cqrsx::cqrs::command::CommandInfo::name(command),
                    }
                }
                fn command_id(&self) -> ::core::option::Option<::cqrsx::__private::Uuid> {
                    match *self {
                        Self::BookSeat(ref command) => ::cqrsx::cqrs::command::CommandInfo::command_id(command),
                        Self::CancelBooking(ref command) => ::cqrsx::cqrs::command::CommandInfo::command_id(command),
                    }
                }
                fn labels(&self) -> ::std::vec::Vec<(&'static str, ::std::string::String)> {
                    match *self {
                        Self::BookSeat(ref command) => ::cqrsx::cqrs::command::CommandInfo::labels(command),
                        Self::CancelBooking(ref command) => ::cqrsx::cqrs::command::CommandInfo::labels(command),
                    }
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn a_generic_enum_of_commands_keeps_its_parameters_and_bounds_next_to_the_aggregate() {
        let expanded = expand_commands(parse_quote! {
            enum Wrapped<'a, T: Clone> where T: Send { One(Note<'a, T>) }
        })
        .unwrap()
        .to_string();
        let decide = quote! {
            impl<'a, T: Clone, __A> ::cqrsx::cqrs::command::decide::Decide<__A> for Wrapped<'a, T>
            where
                T: Send,
                __A: ::cqrsx::cqrs::aggregate::Aggregate,
                Note<'a, T>: ::cqrsx::cqrs::command::decide::Decide<__A>
        };
        assert!(expanded.contains(&decide.to_string()), "{expanded}");
        let info = quote! {
            impl<'a, T: Clone> ::cqrsx::cqrs::command::CommandInfo for Wrapped<'a, T>
            where
                T: Send,
                Note<'a, T>: ::cqrsx::cqrs::command::CommandInfo
        };
        assert!(expanded.contains(&info.to_string()), "{expanded}");
    }

    #[test]
    fn an_enum_of_commands_without_variants_decides_nothing() {
        let expanded = expand_commands(parse_quote! { enum Nothing {} }).unwrap().to_string();
        let decide = quote! { let _ = (state, rules); match self {} };
        assert!(expanded.contains(&decide.to_string()), "{expanded}");
        assert!(
            expanded.contains(&quote! { fn name(&self) -> &'static str { match *self {} } }.to_string()),
            "{expanded}"
        );
    }

    #[test]
    fn every_variant_carries_one_command_type_and_commands_go_on_an_enum() {
        let message = "every variant carries one command type: `Variant(CommandType)`";
        assert_eq!(
            refusal(expand_commands(parse_quote! { enum Command { Book { seat: String } } })),
            message
        );
        assert_eq!(refusal(expand_commands(parse_quote! { enum Command { Book } })), message);
        assert_eq!(
            refusal(expand_commands(parse_quote! { enum Command { Book(BookSeat, Seat) } })),
            message
        );
        assert_eq!(
            refusal(expand_commands(parse_quote! { struct BookSeat; })),
            "`#[derive(Commands)]` goes on an enum of command types: a struct command derives `Command`"
        );
    }
}
