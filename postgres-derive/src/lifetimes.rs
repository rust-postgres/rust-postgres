use crate::overrides::Overrides;
use syn::{AngleBracketedGenericArguments, GenericArgument, Lifetime, PathArguments, Type};

pub(crate) fn extract_borrowed_lifetimes(raw: &syn::Field, overrides: &Overrides) -> Vec<Lifetime> {
    let mut borrowed_lifetimes = vec![];

    // If the field is a reference, it's lifetime should be implicitly borrowed. Serde does
    // the same thing
    if let Type::Reference(ref_type) = &raw.ty {
        let lifetime = &ref_type.lifetime;
        if !borrowed_lifetimes.contains(lifetime.as_ref().unwrap()) {
            borrowed_lifetimes.push(lifetime.to_owned().unwrap());
        }
    }

    // Borrow all generic lifetimes of fields marked with #[postgres(borrow)]
    if overrides.borrows {
        if let Type::Path(type_path) = &raw.ty {
            for segment in &type_path.path.segments {
                if let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                    args, ..
                }) = &segment.arguments
                {
                    for arg in args.iter() {
                        if let GenericArgument::Lifetime(lifetime) = arg {
                            if !borrowed_lifetimes.contains(lifetime) {
                                borrowed_lifetimes.push(lifetime.to_owned());
                            }
                        }
                    }
                }
            }
        }
    }

    borrowed_lifetimes
}
