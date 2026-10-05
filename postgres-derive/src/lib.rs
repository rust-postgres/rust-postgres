//! Internal derive macros for `postgres-types` and `tokio-postgres`.

#![recursion_limit = "256"]
extern crate proc_macro;

use proc_macro::TokenStream;
use syn::parse_macro_input;

mod accepts;
mod case;
mod composites;
mod enums;
#[cfg(feature = "from-row")]
mod from_row;
mod fromsql;
mod overrides;
mod tosql;

#[proc_macro_derive(ToSql, attributes(postgres))]
pub fn derive_tosql(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input);

    tosql::expand_derive_tosql(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(FromSql, attributes(postgres))]
pub fn derive_fromsql(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input);

    fromsql::expand_derive_fromsql(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Derive `tokio_postgres::FromRow` for a struct with named fields.
///
/// Fields are decoded from columns with the same name. Use `#[postgres(name = "column")]` on a
/// field to select another column, or `#[postgres(rename_all = "...")]` on the struct to transform
/// every field name. Supported rename rules are `lowercase`, `UPPERCASE`, `PascalCase`,
/// `camelCase`, `snake_case`, `SCREAMING_SNAKE_CASE`, `kebab-case`, `SCREAMING-KEBAB-CASE`, and
/// `Train-Case`. Explicit field names take precedence over `rename_all`. Raw identifiers use their
/// unprefixed names, so `r#type` maps to `type`.
///
/// A container-level `#[postgres(name = "...")]` is accepted but ignored so the struct can also
/// derive `postgres_types::FromSql` and `postgres_types::ToSql` for a named composite type.
/// Extra row columns are ignored. Missing columns and values with incompatible types return the
/// same errors as `Row::try_get`.
#[cfg(feature = "from-row")]
#[proc_macro_derive(FromRow, attributes(postgres))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input);

    from_row::expand_derive_from_row(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
