use crate::lifetimes;
use crate::overrides::Overrides;
use lifetimes::extract_borrowed_lifetimes;
use syn::{Error, Lifetime};

pub struct UnnamedField {
    pub borrowed_lifetimes: Vec<Lifetime>,
}

impl UnnamedField {
    pub fn parse(raw: &syn::Field) -> Result<UnnamedField, Error> {
        let overrides = Overrides::extract(&raw.attrs, false)?;
        let borrowed_lifetimes = extract_borrowed_lifetimes(raw, &overrides);
        Ok(UnnamedField { borrowed_lifetimes })
    }
}
