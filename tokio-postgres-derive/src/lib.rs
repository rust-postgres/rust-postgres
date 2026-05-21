//! Derive macros for `tokio-postgres`.

#![recursion_limit = "256"]
extern crate proc_macro;

use proc_macro::TokenStream;
use syn::parse_macro_input;

mod attrs;
mod case;
mod from_row;

#[proc_macro_derive(FromRow, attributes(postgres))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input);

    from_row::expand_derive_from_row(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
