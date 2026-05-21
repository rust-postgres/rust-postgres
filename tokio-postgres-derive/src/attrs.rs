use syn::punctuated::Punctuated;
use syn::{Attribute, Error, Expr, ExprLit, Lit, Meta, Token};

use crate::case::{RENAME_RULES, RenameRule};

pub struct ContainerAttrs {
    pub rename_all: Option<RenameRule>,
}

impl ContainerAttrs {
    pub fn extract(attrs: &[Attribute]) -> Result<ContainerAttrs, Error> {
        let mut out = ContainerAttrs { rename_all: None };

        for attr in attrs {
            if !attr.path().is_ident("postgres") {
                continue;
            }

            let list = match &attr.meta {
                Meta::List(list) => list,
                bad => return Err(Error::new_spanned(bad, "expected a #[postgres(...)]")),
            };

            let nested = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;

            for item in nested {
                match item {
                    Meta::NameValue(meta) if meta.path.is_ident("rename_all") => {
                        let value = string_value(&meta.value)?;
                        let rename_rule = RenameRule::from_str(&value).ok_or_else(|| {
                            Error::new_spanned(
                                &meta.value,
                                format!(
                                    "invalid rename_all rule, expected one of: {}",
                                    RENAME_RULES
                                        .iter()
                                        .map(|rule| format!("\"{rule}\""))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                ),
                            )
                        })?;

                        out.rename_all = Some(rename_rule);
                    }
                    Meta::NameValue(meta) if meta.path.is_ident("name") => {
                        return Err(Error::new_spanned(
                            &meta.path,
                            "name is a field attribute for FromRow",
                        ));
                    }
                    Meta::NameValue(meta) => {
                        return Err(Error::new_spanned(&meta.path, "unknown override"));
                    }
                    bad => return Err(Error::new_spanned(bad, "unknown attribute")),
                }
            }
        }

        Ok(out)
    }
}

pub struct FieldAttrs {
    pub name: Option<String>,
}

impl FieldAttrs {
    pub fn extract(attrs: &[Attribute]) -> Result<FieldAttrs, Error> {
        let mut out = FieldAttrs { name: None };

        for attr in attrs {
            if !attr.path().is_ident("postgres") {
                continue;
            }

            let list = match &attr.meta {
                Meta::List(list) => list,
                bad => return Err(Error::new_spanned(bad, "expected a #[postgres(...)]")),
            };

            let nested = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;

            for item in nested {
                match item {
                    Meta::NameValue(meta) if meta.path.is_ident("name") => {
                        out.name = Some(string_value(&meta.value)?);
                    }
                    Meta::NameValue(meta) if meta.path.is_ident("rename_all") => {
                        return Err(Error::new_spanned(
                            &meta.path,
                            "rename_all is a container attribute",
                        ));
                    }
                    Meta::NameValue(meta) => {
                        return Err(Error::new_spanned(&meta.path, "unknown override"));
                    }
                    bad => return Err(Error::new_spanned(bad, "unknown attribute")),
                }
            }
        }

        Ok(out)
    }
}

fn string_value(expr: &Expr) -> Result<String, Error> {
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(lit), ..
        }) => Ok(lit.value()),
        bad => Err(Error::new_spanned(bad, "expected a string literal")),
    }
}
