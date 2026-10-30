---
paths:
  - "**/*.rs"
---

# Rust code guidelines

The [Google Rust Style Guide](https://google.github.io/styleguide/rustguide.html) adapted to this project
(agreed 2026-10-30). It applies to all Rust code: `cqrsx` and `cqrsx-macros` are library code, the demo is
application code where API rules are strong recommendations and simplicity beats generality. Google rules that
are not listed here are not adopted. `/rust-style-review` checks code against these rules; findings cite the
rule numbers.

## Ownership and borrowing

1. `Arc`/`Rc` only when unique ownership with borrowing would be unreasonably complex. Sharing between tasks
   (executors, dependencies, pools) is a reason; replacing a borrow inside one call is not.
2. Borrow instead of a deep copy: `.clone()` only when a copy is really needed, never to appease the borrow
   checker. Cloning an `Arc` is cheap and does not count.
3. A function that only reads an argument takes a reference: taking ownership makes callers clone.
4. Accept the most general borrowed form: `&[T]`, `&str`, `&T`, `Option<&T>`, not `&Vec<T>`, `&String`,
   `&Box<T>`, `&Option<T>`.
5. Return an iterator when callers only walk the result; accept `impl IntoIterator` when the function only walks
   its input. `Vec`/`&[T]` stay when the vector is built anyway, the caller needs ownership or indexing.

## Errors and panics

6. Code outside tests never panics. `Option` is for an absence that needs no reason, `Result` for everything
   else, including a violated internal invariant. Remove a possible panic in this order:
   1. make the invalid state unrepresentable: `NonZero*`, an iterator instead of an index, a retry loop that
      returns its last result instead of `unreachable!` after the loop;
   2. let the compiler check constants: `const MAX_ATTEMPTS: NonZeroUsize = NonZeroUsize::new(16).unwrap();`
      fails the build, not the program; `Default` builds from such typed constants instead of calling a
      validating `new(..).expect(..)`;
   3. return `Err` of the function's error type; a function without `Result` becomes fallible. A violated
      invariant is a bug: log it with `tracing::error!` where it is detected.
7. Hence no `unwrap`, `expect`, `panic!`, `unreachable!`, `todo!`, `unimplemented!` or `assert!` outside tests,
   and no indexing or slicing that can go out of range: use `get()`, iterators and patterns. Values given by the
   caller (version strings, names) are checked by a `try_new` that returns `Result`.
8. Exceptions: tests and the test helpers (`cqrs::testing`), where a panic is the failed check; such a public
   helper documents it in `# Panics` and its assertion message explains the violation instead of repeating the
   condition. A panic in user code (a handler) is caught at the boundary and becomes an error (rule 11).
9. Pattern matching instead of check-then-unwrap: `if let`, `let … else`, `match`.
10. Mutexes: under a `std::sync::Mutex` only short operations, no user code and no `.await`. Lock through one
    shared helper: on poison it logs `error!` naming the lock, calls `clear_poison()` and continues. A lock that
    guards a change of several steps (data may stay half-updated) does not recover: it returns `Err`.
11. `catch_unwind` only at the boundary with user code (handlers run by long-lived workers, where a handler's
    panic must not kill the worker) and in tests; never as control flow inside the engine.

## API and types

12. An `enum` instead of a `bool` parameter whose meaning is not obvious at the call site.
13. A newtype when values of one type with different meanings travel side by side (`event_type` and
    `event_version`, `sequence` and `version`, `task_id` and `command_id`) or a value has rules of its own
    (validation, format). Do not wrap every primitive: settings stay plain, their parameter or builder names are
    enough. `aggregate_id` stays `&str`/`String` in the typical-scenario API until a real mix-up shows up. A type
    alias only shortens a long type (`Outcome<A>`), it never expresses meaning.
14. Named fields instead of tuples and multi-field tuple structs, except newtypes and very local code.
15. Standard traits instead of look-alike methods (`FromStr`, `Display`, `From`); `#[derive]` when it fits. A
    manual impl is fine where derive would add wrong bounds on generic parameters (`CommitReport`).
16. Every public type implements `Debug`; other traits are added when needed. Option structs and types with
    `new()` implement `Default`.
17. A trait exists only for generic code, `impl Trait`/`dyn Trait`, or to extend a foreign type (suffix `Ext`);
    never to group methods.
18. Generics by default; `dyn` for heterogeneous sets (handlers of different types, observers).
19. `impl Into<T>` only in public entry API where the call site becomes clearly cleaner (names as
    `impl Into<String>`); internal code takes concrete types.
20. `#[must_use]` where ignoring the result is a bug: builder methods returning `Self`, pure functions, guards.
    Not on everything.
21. A `match` on our own enum lists the variants that carry meaning instead of hiding them behind `_`, so that a
    new variant breaks compilation where it must be handled. `_` is fine on foreign enums (sqlx, MongoDB
    errors). No `#[non_exhaustive]`: there is one consumer, no backward compatibility, and the compile error is
    the signal we want.

## Naming

22. Standard library conventions: `as_`/`to_`/`into_` with the matching `self`; getters without `get_`;
    constructors `new`, `try_new` when fallible, `from_*` for conversions; `with_*` is a builder method.
    A constructor that can fail: first remove the failure — parameter types that are already valid (`NonZeroU16`,
    a newtype) or a compile-time check of constants (`const { assert!(…) }`) leave a plain `new`; a conversion from
    one value is `FromStr`/`TryFrom`; otherwise `try_new` returning `Result` with its own error type (`Option` only
    for one obvious reason); many checked parameters go through a builder whose `build()` returns `Result`.
23. Generic parameters named after their main bound (`A: Aggregate`, `Q: TaskQueue`); descriptive lifetimes
    beyond a local `'a`.
24. No abbreviations in public API names; common local ones (`tx`, `err`, `ctx`) are fine.
25. A file is named after its primary item unless the name repeats its path: `concert/domain/aggregate.rs`,
    never `concert/domain/concert.rs`.

## Modules and imports

26. A parent module (`foo.rs` next to `foo/`) holds `mod`/`pub use`, the module's central trait or enum and
    small types its children share. A type with a substantial implementation lives in its own file.
27. No glob imports except `use super::*` in tests and well-known preludes (`proptest::prelude::*`).
28. Types and traits are imported and used unqualified; ambiguous names keep their module (`io::Error`).

## Code

29. Iterator chains: never in a `for` header (bind the chain to a variable first); no long chains with
    conditions or mutation of outer state, write a `for` loop; no `for_each`. Short chains and combinators that
    carry the logic (`any`, `map`) are preferred.
30. Spell out the type of a `.into()`, `.parse()` or `.collect()` result when the line does not show it; a `let`
    annotation or a turbofish, either.
31. `format!`/`write!` instead of `+` for more than two strings and instead of `str::replace` templating.
32. No `unsafe`, no unstable features. `#[rustfmt::skip]` only for alignment with a readability reason.
33. A derive or procedural macro implements an ordinary public trait that can be written by hand
    (`CommandInfo`, `Decide`, `EventKind`, …). Derives are welcome where they remove boilerplate from
    application code.

## Documentation

34. Modules, public types, functions and methods are documented: what and why, never the name restated. Enum
    variants and fields only when the name is not enough. Comments add what the code does not say. The
    `missing_docs` lint stays off: it would demand restated names on every variant and field.

## Not adopted

`#[non_exhaustive]`; macros as a last resort; small crates and a flat file layout; one module per `use`
(`imports_granularity = Module`); trait imports inside function bodies; `module::function()` calls;
`max_width = 100` (we keep 130); `with_foo()` as a closure accessor; caution with `const fn`; `Copy` error
types; rules for Chromium, Fuchsia and Android only (no async, no threads, `fuchsia_sync::Mutex`, their test
frameworks, the `catch_unwind` ban that comes from `panic=abort`).
