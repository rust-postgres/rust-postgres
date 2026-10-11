use syn::punctuated::Punctuated;
use syn::{Attribute, Error, Expr, ExprLit, Lit, Meta, Token};

use crate::case::{RENAME_RULES, RenameRule};

pub struct Overrides {
    pub name: Option<String>,
    pub schema: Option<String>,
    pub rename_all: Option<RenameRule>,
    pub transparent: bool,
    pub allow_mismatch: bool,
}

impl Overrides {
    pub fn extract(attrs: &[Attribute], container_attr: bool) -> Result<Overrides, Error> {
        let mut overrides = Overrides {
            name: None,
            schema: None,
            rename_all: None,
            transparent: false,
            allow_mismatch: false,
        };

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
                    Meta::NameValue(meta) => {
                        enum Key {
                            Name,
                            Schema,
                            RenameAll,
                        }

                        let key = if meta.path.is_ident("name") {
                            Key::Name
                        } else if meta.path.is_ident("schema") {
                            Key::Schema
                        } else if meta.path.is_ident("rename_all") {
                            Key::RenameAll
                        } else {
                            return Err(Error::new_spanned(&meta.path, "unknown override"));
                        };

                        if !container_attr {
                            // None if the attr is permitted on non-containers
                            let msg = match key {
                                Key::Name => None,
                                Key::Schema => Some("schema is a container attribute"),
                                Key::RenameAll => Some("rename_all is a container attribute"),
                            };
                            if let Some(msg) = msg {
                                return Err(Error::new_spanned(&meta.path, msg));
                            }
                        }

                        let value = match &meta.value {
                            Expr::Lit(ExprLit {
                                lit: Lit::Str(lit), ..
                            }) => lit.value(),
                            bad => {
                                return Err(Error::new_spanned(bad, "expected a string literal"));
                            }
                        };

                        match key {
                            Key::Name => {
                                overrides.name = Some(value);
                            }
                            Key::Schema => {
                                overrides.schema = Some(value);
                            }
                            Key::RenameAll => {
                                let rename_rule =
                                    RenameRule::from_str(&value).ok_or_else(|| {
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

                                overrides.rename_all = Some(rename_rule);
                            }
                        }
                    }
                    Meta::Path(path) => {
                        if path.is_ident("transparent") {
                            if overrides.allow_mismatch {
                                return Err(Error::new_spanned(
                                    path,
                                    "#[postgres(allow_mismatch)] is not allowed with #[postgres(transparent)]",
                                ));
                            }
                            overrides.transparent = true;
                        } else if path.is_ident("allow_mismatch") {
                            if overrides.transparent {
                                return Err(Error::new_spanned(
                                    path,
                                    "#[postgres(transparent)] is not allowed with #[postgres(allow_mismatch)]",
                                ));
                            }
                            overrides.allow_mismatch = true;
                        } else {
                            return Err(Error::new_spanned(path, "unknown override"));
                        }
                    }
                    bad => return Err(Error::new_spanned(bad, "unknown attribute")),
                }
            }
        }

        Ok(overrides)
    }
}
