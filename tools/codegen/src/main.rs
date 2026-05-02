#![warn(clippy::all)]
#![allow(clippy::write_with_newline)]

extern crate linked_hash_map;
extern crate marksman_escape;
extern crate phf_codegen;
extern crate regex;

mod sqlstate;
mod type_gen;

fn main() {
    sqlstate::build();
    type_gen::build();
}

fn snake_to_camel(s: &str) -> String {
    let mut out = String::new();

    let mut upper = true;
    for ch in s.chars() {
        if ch == '_' {
            upper = true;
        } else {
            let ch = if upper {
                upper = false;
                ch.to_ascii_uppercase()
            } else {
                ch
            };
            out.push(ch);
        }
    }

    out
}
