use lifetimes::extract_borrowed_lifetimes;
use proc_macro2::Span;
use syn::{
    punctuated::Punctuated, Error, GenericParam, Generics, Ident, Lifetime, Path, PathSegment,
    Type, TypeParamBound,
};

use crate::{case::RenameRule, lifetimes, overrides::Overrides};

pub struct NamedField {
    pub name: String,
    pub ident: Ident,
    pub type_: Type,
    pub borrowed_lifetimes: Vec<Lifetime>,
}

impl NamedField {
    pub fn parse(raw: &syn::Field, rename_all: Option<RenameRule>) -> Result<NamedField, Error> {
        let overrides = Overrides::extract(&raw.attrs, false)?;
        let ident = raw.ident.as_ref().unwrap().clone();

        let borrowed_lifetimes = extract_borrowed_lifetimes(raw, &overrides);

        // field level name override takes precedence over container level rename_all override
        let name = match overrides.name {
            Some(n) => n,
            None => {
                let name = ident.to_string();
                let stripped = name.strip_prefix("r#").map(String::from).unwrap_or(name);

                match rename_all {
                    Some(rule) => rule.apply_to_field(&stripped),
                    None => stripped,
                }
            }
        };

        Ok(NamedField {
            name,
            ident,
            type_: raw.ty.clone(),
            borrowed_lifetimes,
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
