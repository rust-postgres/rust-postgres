use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream, TokenTree};
use quote::{ToTokens, format_ident, quote};
use syn::{
    Data, DataStruct, DeriveInput, Error, Fields, GenericParam, Ident, Lifetime, LifetimeParam,
    ext::IdentExt, parse_quote,
};

use crate::composites::Field;
use crate::overrides::{Derive, Overrides};

pub fn expand_derive_from_row(input: DeriveInput) -> Result<TokenStream, Error> {
    let overrides = Overrides::extract(&input.attrs, true, Derive::Row)?;

    let Data::Struct(DataStruct {
        fields: Fields::Named(fields),
        ..
    }) = &input.data
    else {
        return Err(Error::new_spanned(
            &input,
            "#[derive(FromRow)] may only be applied to structs with named fields",
        ));
    };
    let fields = fields
        .named
        .iter()
        .map(|field| Field::parse(field, overrides.rename_all, Derive::Row))
        .collect::<Result<Vec<_>, _>>()?;

    let ident = &input.ident;
    let postgres = postgres_path(ident.span())?;
    let prefix = private_prefix(&input);
    let row = format_ident!("{prefix}_row");
    let column = format_ident!("{prefix}_column");
    let decode_field = format_ident!("{prefix}_decode_field");
    // Adding a separate lifetime; the struct's own lifetimes need not borrow from the row.
    let row_lifetime = Lifetime::new(&format!("'{row}"), Span::call_site());
    let mut generics = input.generics.clone();
    generics.params.insert(
        0,
        GenericParam::Lifetime(LifetimeParam::new(row_lifetime.clone())),
    );

    // Indexing the bounds; direct `FromSql` bounds can overlap with other fields or input bounds.
    for (index, field) in fields.iter().enumerate() {
        let field_type = &field.type_;
        generics
            .make_where_clause()
            .predicates
            .push(parse_quote!(#field_type: #decode_field<#row_lifetime, #index>));
    }

    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, type_generics, _) = input.generics.split_for_impl();
    let field_idents = fields.iter().map(|field| &field.ident);
    let field_names = fields.iter().map(|field| &field.name);
    let field_types = fields.iter().map(|field| &field.type_);
    let field_indices = 0..fields.len();
    let field_bindings = field_indices
        .clone()
        .map(|index| format_ident!("{prefix}_field{index}"))
        .collect::<Vec<_>>();

    Ok(quote! {
        const _: () = {
            trait #decode_field<#row_lifetime, const INDEX: ::core::primitive::usize>: ::core::marker::Sized {
                fn decode(
                    #row: &#row_lifetime #postgres::Row,
                    #column: &::core::primitive::str,
                ) -> ::core::result::Result<Self, #postgres::Error>;
            }

            impl<#row_lifetime, T, const INDEX: ::core::primitive::usize> #decode_field<#row_lifetime, { INDEX }> for T
            where
                T: #postgres::types::FromSql<#row_lifetime>,
            {
                fn decode(
                    #row: &#row_lifetime #postgres::Row,
                    #column: &::core::primitive::str,
                ) -> ::core::result::Result<Self, #postgres::Error> {
                    #row.try_get(#column)
                }
            }

            impl #impl_generics #postgres::FromRow<#row_lifetime> for #ident #type_generics #where_clause {
                fn from_row(#row: &#row_lifetime #postgres::Row) -> ::core::result::Result<Self, #postgres::Error> {
                    #(
                        let #field_bindings = <#field_types as #decode_field<#row_lifetime, #field_indices>>
                            ::decode(#row, #field_names)?;
                    )*
                    ::core::result::Result::Ok(Self {
                        #(
                            #field_idents: #field_bindings,
                        )*
                    })
                }
            }
        };
    })
}

/// Choose a prefix that cannot collide with input bindings or higher-ranked lifetimes.
fn private_prefix(input: &DeriveInput) -> String {
    let mut prefix = "__tokio_postgres".to_owned();
    let mut streams = vec![input.to_token_stream().into_iter()];

    while let Some(stream) = streams.last_mut() {
        match stream.next() {
            None => {
                streams.pop();
            }
            Some(TokenTree::Group(group)) => streams.push(group.stream().into_iter()),
            Some(TokenTree::Ident(ident)) => {
                let ident = ident.unraw().to_string();
                while ident.starts_with(&prefix) {
                    prefix.push('_');
                }
            }
            _ => {}
        }
    }

    prefix
}

/// Resolve the absolute tokio-postgres path using the consuming crate's dependency name.
fn postgres_path(span: Span) -> Result<TokenStream, Error> {
    let name = crate_name("tokio-postgres");
    let name = name.map_err(|error| Error::new(span, error))?;
    match name {
        FoundCrate::Itself => Ok(quote!(::tokio_postgres)),
        FoundCrate::Name(name) => {
            let name = Ident::new_raw(&name, span);
            Ok(quote!(::#name))
        }
    }
}
