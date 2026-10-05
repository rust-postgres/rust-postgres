use proc_macro2::Span;
use syn::{
    Error, GenericParam, Generics, Ident, Path, PathSegment, Type, TypeParamBound, ext::IdentExt,
    punctuated::Punctuated,
};

use crate::case::RenameRule;
use crate::overrides::{Derive, Overrides};

/// Named Rust field mapped to a PostgreSQL composite field or row column.
pub struct Field {
    pub name: String,
    pub ident: Ident,
    pub type_: Type,
}

impl Field {
    pub fn parse(
        raw: &syn::Field,
        rename_all: Option<RenameRule>,
        derive: Derive,
    ) -> Result<Field, Error> {
        let overrides = Overrides::extract(&raw.attrs, false, derive)?;
        let ident = raw.ident.as_ref().unwrap().clone();

        let name = match overrides.name {
            Some(n) => n,
            None => {
                let name = ident.unraw().to_string();

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

pub(crate) fn append_generic_bound(mut generics: Generics, bound: &TypeParamBound) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(param) = param {
            param.bounds.push(bound.to_owned())
        }
    }
    generics
}

pub(crate) fn new_derive_path(last: PathSegment) -> Path {
    let mut path = Path {
        leading_colon: None,
        segments: Punctuated::new(),
    };
    path.segments
        .push(Ident::new("postgres_types", Span::call_site()).into());
    path.segments.push(last);
    path
}
