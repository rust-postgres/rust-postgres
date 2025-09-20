use proc_macro2::{Span, TokenStream};
use quote::quote;
use std::iter;
use syn::{
    Data, DataStruct, DeriveInput, Error, Fields, Ident, TraitBound, TraitBoundModifier,
    TypeParamBound,
};

use crate::accepts;
use crate::composites::Field;
use crate::composites::{append_generic_bound, new_derive_path};
use crate::enums::Variant;
use crate::overrides::Overrides;

pub fn expand_derive_tosql(input: DeriveInput) -> Result<TokenStream, Error> {
    let overrides = Overrides::extract(&input.attrs, true)?;

    if (overrides.name.is_some() || overrides.rename_all.is_some()) && overrides.transparent {
        return Err(Error::new_spanned(
            &input,
            "#[postgres(transparent)] is not allowed with #[postgres(name = \"...\")] or #[postgres(rename_all = \"...\")]",
        ));
    }

    let name = overrides
        .name
        .clone()
        .unwrap_or_else(|| input.ident.to_string());

    let (accepts_body, to_sql_body) = if overrides.transparent {
        match input.data {
            Data::Struct(DataStruct {
                fields: Fields::Unnamed(ref fields),
                ..
            }) if fields.unnamed.len() == 1 => {
                let field = fields.unnamed.first().unwrap();

                (accepts::transparent_body(field), transparent_body())
            }
            _ => {
                return Err(Error::new_spanned(
                    input,
                    "#[postgres(transparent)] may only be applied to single field tuple structs",
                ));
            }
        }
    } else if overrides.allow_mismatch {
        match input.data {
            Data::Enum(ref data) => {
                let variants = data
                    .variants
                    .iter()
                    .map(|variant| Variant::parse(variant, overrides.rename_all))
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    accepts::enum_body(&name, &variants, overrides.allow_mismatch),
                    enum_body(&input.ident, &variants),
                )
            }
            _ => {
                return Err(Error::new_spanned(
                    input,
                    "#[postgres(allow_mismatch)] may only be applied to enums",
                ));
            }
        }
    } else {
        match input.data {
            Data::Enum(ref data) => {
                let variants = data
                    .variants
                    .iter()
                    .map(|variant| Variant::parse(variant, overrides.rename_all))
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    accepts::enum_body(&name, &variants, overrides.allow_mismatch),
                    enum_body(&input.ident, &variants),
                )
            }
            Data::Struct(DataStruct {
                fields: Fields::Unnamed(ref fields),
                ..
            }) if fields.unnamed.len() == 1 => {
                let field = fields.unnamed.first().unwrap();

                (accepts::domain_body(&name, field), domain_body())
            }
            Data::Struct(DataStruct {
                fields: Fields::Named(ref fields),
                ..
            }) => {
                let fields = fields
                    .named
                    .iter()
                    .map(|field| Field::parse(field, overrides.rename_all))
                    .collect::<Result<Vec<_>, _>>()?;
                (
                    accepts::composite_body(&name, "ToSql", &fields),
                    composite_body(&fields),
                )
            }
            _ => {
                return Err(Error::new_spanned(
                    input,
                    "#[derive(ToSql)] may only be applied to structs, single field tuple structs, and enums",
                ));
            }
        }
    };

    let ident = &input.ident;
    let generics = append_generic_bound(input.generics.to_owned(), &new_tosql_bound());
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let out = quote! {
        impl#impl_generics postgres_types::ToSql for #ident#ty_generics #where_clause {
            fn to_sql(&self,
                      _type: &postgres_types::Type,
                      buf: &mut postgres_types::private::BytesMut)
                      -> std::result::Result<postgres_types::IsNull,
                                             std::boxed::Box<std::error::Error +
                                                             std::marker::Sync +
                                                             std::marker::Send>> {
                #to_sql_body
            }

            fn accepts(type_: &postgres_types::Type) -> bool {
                #accepts_body
            }

            postgres_types::to_sql_checked!();
        }
    };

    Ok(out)
}

fn transparent_body() -> TokenStream {
    quote! {
        postgres_types::ToSql::to_sql(&self.0, _type, buf)
    }
}

fn enum_body(ident: &Ident, variants: &[Variant]) -> TokenStream {
    let idents = iter::repeat(ident);
    let variant_idents = variants.iter().map(|v| &v.ident);
    let variant_names = variants.iter().map(|v| &v.name);

    quote! {
        let s = match *self {
            #(
                #idents::#variant_idents => #variant_names,
            )*
        };

        buf.extend_from_slice(s.as_bytes());
        std::result::Result::Ok(postgres_types::IsNull::No)
    }
}

fn domain_body() -> TokenStream {
    quote! {
        let type_ = match *_type.kind() {
            postgres_types::Kind::Domain(ref type_) => type_,
            _ => unreachable!(),
        };

        postgres_types::ToSql::to_sql(&self.0, type_, buf)
    }
}

fn composite_body(fields: &[Field]) -> TokenStream {
    let field_names = fields.iter().map(|f| &f.name);
    let field_idents = fields.iter().map(|f| &f.ident);

    quote! {
        let fields = match *_type.kind() {
            postgres_types::Kind::Composite(ref fields) => fields,
            _ => unreachable!(),
        };

        buf.extend_from_slice(&(fields.len() as i32).to_be_bytes());

        for field in fields {
            buf.extend_from_slice(&field.type_().oid().to_be_bytes());

            let base = buf.len();
            buf.extend_from_slice(&[0; 4]);
            let r = match field.name() {
                #(
                    #field_names => postgres_types::ToSql::to_sql(&self.#field_idents, field.type_(), buf),
                )*
                _ => unreachable!(),
            };

            let count = match r? {
                postgres_types::IsNull::Yes => -1,
                postgres_types::IsNull::No => {
                    let len = buf.len() - base - 4;
                    if len > i32::MAX as usize {
                        return std::result::Result::Err(
                            std::convert::Into::into("value too large to transmit"));
                    }
                    len as i32
                }
            };

            buf[base..base + 4].copy_from_slice(&count.to_be_bytes());
        }

        std::result::Result::Ok(postgres_types::IsNull::No)
    }
}

fn new_tosql_bound() -> TypeParamBound {
    TypeParamBound::Trait(TraitBound {
        lifetimes: None,
        modifier: TraitBoundModifier::None,
        paren_token: None,
        path: new_derive_path(Ident::new("ToSql", Span::call_site()).into()),
    })
}
