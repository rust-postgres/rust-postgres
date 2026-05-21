use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    Data, DataStruct, DeriveInput, Error, Fields, GenericParam, Generics, Ident, Lifetime,
    LifetimeParam, Type, parse_quote,
};

use crate::attrs::{ContainerAttrs, FieldAttrs};
use crate::case::RenameRule;

pub fn expand_derive_from_row(input: DeriveInput) -> Result<TokenStream, Error> {
    let container_attrs = ContainerAttrs::extract(&input.attrs)?;

    let fields = match input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(ref fields),
            ..
        }) => fields
            .named
            .iter()
            .map(|field| Field::parse(field, container_attrs.rename_all))
            .collect::<Result<Vec<_>, _>>()?,
        _ => {
            return Err(Error::new_spanned(
                input,
                "#[derive(FromRow)] may only be applied to structs with named fields",
            ));
        }
    };

    let ident = &input.ident;
    let (generics, row_lifetime) = build_generics(&input.generics, &fields)?;
    let (impl_generics, _, where_clause) = generics.split_for_impl();
    let (_, ty_generics, _) = input.generics.split_for_impl();
    let body = from_row_body(&fields);

    Ok(quote! {
        impl #impl_generics tokio_postgres::FromRow<#row_lifetime> for #ident #ty_generics #where_clause {
            fn from_row(row: &#row_lifetime tokio_postgres::Row) -> std::result::Result<Self, tokio_postgres::Error> {
                #body
            }
        }
    })
}

struct Field {
    name: String,
    ident: Ident,
    type_: Type,
}

impl Field {
    fn parse(raw: &syn::Field, rename_all: Option<RenameRule>) -> Result<Field, Error> {
        let attrs = FieldAttrs::extract(&raw.attrs)?;
        let ident = raw.ident.as_ref().unwrap().clone();
        let name = match attrs.name {
            Some(name) => name,
            None => {
                let name = ident.to_string();
                let name = name.strip_prefix("r#").map(String::from).unwrap_or(name);

                match rename_all {
                    Some(rule) => rule.apply_to_field(&name),
                    None => name,
                }
            }
        };

        Ok(Field {
            name,
            ident,
            type_: raw.ty.clone(),
        })
    }
}

fn from_row_body(fields: &[Field]) -> TokenStream {
    let field_idents = fields.iter().map(|f| &f.ident);
    let field_names = fields.iter().map(|f| &f.name);
    let field_types = fields.iter().map(|f| &f.type_);

    quote! {
        std::result::Result::Ok(Self {
            #(
                #field_idents: row.try_get::<_, #field_types>(#field_names)?,
            )*
        })
    }
}

fn build_generics(source: &Generics, fields: &[Field]) -> Result<(Generics, Lifetime), Error> {
    let mut lifetimes = source.lifetimes();
    let row_lifetime = match (lifetimes.next(), lifetimes.next()) {
        (Some(lifetime), None) => lifetime.lifetime.clone(),
        (None, None) => Lifetime::new("'__row", Span::call_site()),
        _ => {
            return Err(Error::new_spanned(
                source,
                "#[derive(FromRow)] supports at most one lifetime parameter",
            ));
        }
    };

    let mut out = source.to_owned();

    if source.lifetimes().next().is_none() {
        out.params.insert(
            0,
            GenericParam::Lifetime(LifetimeParam::new(row_lifetime.to_owned())),
        );
    }

    for field in fields {
        let field_type = &field.type_;
        out.make_where_clause()
            .predicates
            .push(parse_quote!(#field_type: tokio_postgres::types::FromSql<#row_lifetime>));
    }

    Ok((out, row_lifetime))
}
