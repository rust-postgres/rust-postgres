use proc_macro2::{Span, TokenStream};
use quote::quote;
use std::iter;
use syn::Ident;

use crate::composites::NamedField;
use crate::enums::Variant;

pub fn transparent_body(field: &syn::Field) -> TokenStream {
    let ty = &field.ty;

    quote! {
        <#ty as ::postgres_types::ToSql>::accepts(type_)
    }
}

pub fn domain_body(name: &str, field: &syn::Field) -> TokenStream {
    let ty = &field.ty;

    quote! {
        if type_.name() != #name {
            return false;
        }

        match *type_.kind() {
            ::postgres_types::Kind::Domain(ref type_) => {
                <#ty as ::postgres_types::ToSql>::accepts(type_)
            }
            _ => false,
        }
    }
}

pub fn enum_body(name: &str, variants: &[Variant], allow_mismatch: bool) -> TokenStream {
    let num_variants = variants.len();
    let variant_names = variants.iter().map(|v| &v.name);

    if allow_mismatch {
        quote! {
            type_.name() == #name
        }
    } else {
        quote! {
            if type_.name() != #name {
                return false;
            }

            match *type_.kind() {
                ::postgres_types::Kind::Enum(ref variants) => {
                    if variants.len() != #num_variants {
                        return false;
                    }

                    variants.iter().all(|v| {
                        match &**v {
                            #(
                                #variant_names => true,
                            )*
                            _ => false,
                        }
                    })
                }
                _ => false,
            }
        }
    }
}

pub fn composite_body(name: &str, trait_: &str, fields: &[NamedField]) -> TokenStream {
    let num_fields = fields.len();
    let trait_ = Ident::new(trait_, Span::call_site());
    let traits = iter::repeat(&trait_);
    let field_names = fields.iter().map(|f| &f.name);
    let field_types = fields.iter().map(|f| &f.type_);

    quote! {
        if type_.name() != #name {
            return false;
        }

        match *type_.kind() {
            ::postgres_types::Kind::Composite(ref fields) => {
                if fields.len() != #num_fields {
                    return false;
                }

                fields.iter().all(|f| {
                    match f.name() {
                        #(
                            #field_names => {
                                <#field_types as ::postgres_types::#traits>::accepts(f.type_())
                            }
                        )*
                        _ => false,
                    }
                })
            }
            _ => false,
        }
    }
}
