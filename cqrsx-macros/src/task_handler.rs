//! `#[task_handler]`: a task handler written as a function of its task, its context and its dependencies.
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, FnArg, ItemFn, PatType, ReturnType, Type, TypeReference};

/// A function `async fn name(task: T, …) -> TaskResult` becomes the value `name`, a handler of the tasks `T`: a unit
/// struct of the function's name with the function as its method `call` and an implementation of
/// `::cqrsx::task::handler::TaskHandler<T>`. The first parameter is the task, by value; a parameter `&TaskContext` is
/// the attempt's context, wherever it stands; every other parameter `&D` is a dependency, taken from the context by
/// its type and listed in `DEPENDENCIES` for the assembly to check.
pub(crate) fn expand_task_handler(attribute: TokenStream, function: ItemFn) -> syn::Result<TokenStream> {
    if !attribute.is_empty() {
        return Err(Error::new_spanned(attribute, "`#[task_handler]` takes no arguments"));
    }
    let signature = &function.sig;
    if signature.asyncness.is_none() {
        return Err(Error::new_spanned(signature.fn_token, "a task handler is an `async fn`"));
    }
    if !signature.generics.params.is_empty() || signature.generics.where_clause.is_some() {
        return Err(Error::new_spanned(&signature.generics, "a task handler is not generic"));
    }
    if matches!(signature.output, ReturnType::Default) {
        return Err(Error::new_spanned(&signature.ident, "a task handler answers a `TaskResult`"));
    }
    let mut inputs = signature.inputs.iter();
    let task = match inputs.next() {
        None => {
            return Err(Error::new_spanned(
                &signature.ident,
                "a task handler takes its task as its first parameter: `task: MyTask`",
            ));
        }
        Some(FnArg::Typed(PatType { ty, .. })) if !matches!(**ty, Type::Reference(_)) => ty,
        Some(input) => {
            return Err(refusal(
                input,
                "a task handler takes its task by value as its first parameter",
            ));
        }
    };
    let (mut arguments, mut resolved, mut dependencies) = (vec![quote! { task }], Vec::new(), Vec::new());
    for (index, input) in inputs.enumerate() {
        let elem = match input {
            FnArg::Typed(PatType { ty, .. }) => match &**ty {
                Type::Reference(TypeReference {
                    elem, mutability: None, ..
                }) => elem,
                _ => {
                    return Err(refusal(
                        input,
                        "a task handler takes its context as `&TaskContext` and every dependency by reference: `mail: &Mailer`",
                    ));
                }
            },
            FnArg::Receiver(_) => return Err(refusal(input, "a task handler is a function, not a method")),
        };
        if is_context(elem) {
            arguments.push(quote! { ctx });
            continue;
        }
        let name = format_ident!("dependency_{}", index + 1);
        resolved.push(quote! {
            let #name = match ctx.dependency::<#elem>() {
                ::core::result::Result::Ok(dependency) => dependency,
                ::core::result::Result::Err(missing) => {
                    return ::cqrsx::task::result::TaskResult::fail(::std::string::ToString::to_string(&missing));
                }
            };
        });
        dependencies.push(quote! { ::cqrsx::task::dependencies::DependencyId::of::<#elem>() });
        arguments.push(quote! { #name });
    }
    let (vis, ident, inputs, output, block) = (
        &function.vis,
        &signature.ident,
        &signature.inputs,
        &signature.output,
        &function.block,
    );
    let docs = function.attrs.iter().filter(|attribute| attribute.path().is_ident("doc"));
    let others = function.attrs.iter().filter(|attribute| !attribute.path().is_ident("doc"));
    Ok(quote! {
        #(#docs)*
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug, Default)]
        #vis struct #ident;

        impl #ident {
            /// The handler as written: its task, its context and its dependencies, in their order.
            #(#others)*
            #vis async fn call(&self, #inputs) #output #block
        }

        impl ::cqrsx::task::handler::TaskHandler<#task> for #ident {
            const DEPENDENCIES: &'static [::cqrsx::task::dependencies::DependencyId] = &[#(#dependencies),*];

            #[allow(unused_variables)]
            async fn handle(
                &self,
                task: #task,
                ctx: &::cqrsx::task::context::TaskContext,
            ) -> ::cqrsx::task::result::TaskResult {
                #(#resolved)*
                self.call(#(#arguments),*).await
            }
        }
    })
}

/// Whether `ty` names the context of a task: a path ending in `TaskContext`.
fn is_context(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.segments.last().is_some_and(|last| last.ident == "TaskContext"))
}

fn refusal(input: &FnArg, message: &str) -> Error {
    Error::new_spanned(input, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn refusal(attribute: TokenStream, function: ItemFn) -> String {
        expand_task_handler(attribute, function).unwrap_err().to_string()
    }

    #[test]
    fn a_handler_function_becomes_a_value_with_its_call_and_its_handler() {
        let expanded = expand_task_handler(
            TokenStream::new(),
            parse_quote! {
                /// Refunds a cancellation.
                #[allow(clippy::needless_pass_by_value)]
                pub async fn issue_refund(task: IssueRefund, ctx: &TaskContext, mail: &Mailer) -> TaskResult {
                    mail.send(ctx, refund_mail(&task)).await
                }
            },
        )
        .unwrap();
        let expected = quote! {
            /// Refunds a cancellation.
            #[allow(non_camel_case_types)]
            #[derive(Clone, Copy, Debug, Default)]
            pub struct issue_refund;

            impl issue_refund {
                /// The handler as written: its task, its context and its dependencies, in their order.
                #[allow(clippy::needless_pass_by_value)]
                pub async fn call(&self, task: IssueRefund, ctx: &TaskContext, mail: &Mailer) -> TaskResult {
                    mail.send(ctx, refund_mail(&task)).await
                }
            }

            impl ::cqrsx::task::handler::TaskHandler<IssueRefund> for issue_refund {
                const DEPENDENCIES: &'static [::cqrsx::task::dependencies::DependencyId] =
                    &[::cqrsx::task::dependencies::DependencyId::of::<Mailer>()];

                #[allow(unused_variables)]
                async fn handle(
                    &self,
                    task: IssueRefund,
                    ctx: &::cqrsx::task::context::TaskContext,
                ) -> ::cqrsx::task::result::TaskResult {
                    let dependency_2 = match ctx.dependency::<Mailer>() {
                        ::core::result::Result::Ok(dependency) => dependency,
                        ::core::result::Result::Err(missing) => {
                            return ::cqrsx::task::result::TaskResult::fail(::std::string::ToString::to_string(&missing));
                        }
                    };
                    self.call(task, ctx, dependency_2).await
                }
            }
        };
        assert_eq!(expanded.to_string(), expected.to_string());
    }

    #[test]
    fn the_context_and_the_dependencies_are_found_by_type_in_any_order() {
        let expanded = expand_task_handler(
            TokenStream::new(),
            parse_quote! {
                async fn audit(entry: Entry, clock: &dyn Clock, context: &cqrsx::task::context::TaskContext, log: &Log) -> TaskResult {
                    TaskResult::done()
                }
            },
        )
        .unwrap()
        .to_string();
        let dependencies = quote! {
            &[
                ::cqrsx::task::dependencies::DependencyId::of::<dyn Clock>(),
                ::cqrsx::task::dependencies::DependencyId::of::<Log>()
            ]
        };
        assert!(expanded.contains(&dependencies.to_string()), "{expanded}");
        let call = quote! { self.call(task, dependency_1, ctx, dependency_3).await };
        assert!(expanded.contains(&call.to_string()), "{expanded}");
        assert!(
            expanded.contains(&quote! { struct audit; }.to_string()),
            "a private function, a private value: {expanded}"
        );
        let alone = expand_task_handler(
            TokenStream::new(),
            parse_quote! { pub(crate) async fn ping(task: Ping) -> TaskResult { TaskResult::done() } },
        )
        .unwrap()
        .to_string();
        assert!(alone.contains(&quote! { = &[]; }.to_string()), "no dependencies: {alone}");
        assert!(alone.contains(&quote! { self.call(task).await }.to_string()), "{alone}");
        assert!(alone.contains(&quote! { pub(crate) struct ping; }.to_string()), "{alone}");
    }

    #[test]
    fn a_function_without_a_task_a_task_by_reference_a_dependency_by_value_a_receiver_generics_or_arguments_is_refused() {
        let cases: [(TokenStream, ItemFn, &str); 9] = [
            (
                quote! { policy = "mail" },
                parse_quote! { async fn ping(task: Ping) -> TaskResult { todo!() } },
                "`#[task_handler]` takes no arguments",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping() -> TaskResult { todo!() } },
                "a task handler takes its task as its first parameter: `task: MyTask`",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping(task: &Ping) -> TaskResult { todo!() } },
                "a task handler takes its task by value as its first parameter",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping(&self, task: Ping) -> TaskResult { todo!() } },
                "a task handler takes its task by value as its first parameter",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping(task: Ping, mail: Mailer) -> TaskResult { todo!() } },
                "a task handler takes its context as `&TaskContext` and every dependency by reference: `mail: &Mailer`",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping(task: Ping, mail: &mut Mailer) -> TaskResult { todo!() } },
                "a task handler takes its context as `&TaskContext` and every dependency by reference: `mail: &Mailer`",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping<T>(task: T) -> TaskResult { todo!() } },
                "a task handler is not generic",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping(task: Ping) where Ping: Send { todo!() } },
                "a task handler is not generic",
            ),
            (
                TokenStream::new(),
                parse_quote! { async fn ping(task: Ping) { todo!() } },
                "a task handler answers a `TaskResult`",
            ),
        ];
        for (attribute, function, message) in cases {
            assert_eq!(refusal(attribute, function), message);
        }
        let mut method: ItemFn = parse_quote! { async fn ping(task: Ping) -> TaskResult { todo!() } };
        method.sig.inputs.push(parse_quote! { &self });
        assert_eq!(
            refusal(TokenStream::new(), method),
            "a task handler is a function, not a method"
        );
    }

    #[test]
    fn a_function_that_is_not_async_is_refused() {
        assert_eq!(
            refusal(
                TokenStream::new(),
                parse_quote! { fn ping(task: Ping) -> TaskResult { TaskResult::done() } }
            ),
            "a task handler is an `async fn`"
        );
    }

    #[test]
    fn only_a_reference_to_a_type_named_task_context_is_the_context() {
        assert!(is_context(&parse_quote! { TaskContext }));
        assert!(is_context(&parse_quote! { ::cqrsx::task::context::TaskContext }));
        assert!(!is_context(&parse_quote! { Context }));
        assert!(!is_context(&parse_quote! { <T as Trait>::TaskContext }));
        assert!(!is_context(&parse_quote! { [TaskContext] }));
    }
}
