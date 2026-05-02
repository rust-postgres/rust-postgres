#[allow(deprecated, unused_imports)]
use std::ascii::AsciiExt;

use heck::{
    ToKebabCase, ToLowerCamelCase, ToShoutyKebabCase, ToShoutySnakeCase, ToSnakeCase, ToTrainCase,
    ToUpperCamelCase,
};

use self::RenameRule::*;

/// The different possible ways to change case of fields in a struct, or variants in an enum.
#[allow(clippy::enum_variant_names)]
#[derive(Copy, Clone, PartialEq)]
pub enum RenameRule {
    /// Rename direct children to "lowercase" style.
    LowerCase,
    /// Rename direct children to "UPPERCASE" style.
    UpperCase,
    /// Rename direct children to "PascalCase" style, as typically used for
    /// enum variants.
    PascalCase,
    /// Rename direct children to "camelCase" style.
    CamelCase,
    /// Rename direct children to "snake_case" style, as commonly used for
    /// fields.
    SnakeCase,
    /// Rename direct children to "SCREAMING_SNAKE_CASE" style, as commonly
    /// used for constants.
    ScreamingSnakeCase,
    /// Rename direct children to "kebab-case" style.
    KebabCase,
    /// Rename direct children to "SCREAMING-KEBAB-CASE" style.
    ScreamingKebabCase,

    /// Rename direct children to "Train-Case" style.
    TrainCase,
}

pub const RENAME_RULES: &[&str] = &[
    "lowercase",
    "UPPERCASE",
    "PascalCase",
    "camelCase",
    "snake_case",
    "SCREAMING_SNAKE_CASE",
    "kebab-case",
    "SCREAMING-KEBAB-CASE",
    "Train-Case",
];

impl RenameRule {
    pub fn from_str(rule: &str) -> Option<RenameRule> {
        match rule {
            "lowercase" => Some(LowerCase),
            "UPPERCASE" => Some(UpperCase),
            "PascalCase" => Some(PascalCase),
            "camelCase" => Some(CamelCase),
            "snake_case" => Some(SnakeCase),
            "SCREAMING_SNAKE_CASE" => Some(ScreamingSnakeCase),
            "kebab-case" => Some(KebabCase),
            "SCREAMING-KEBAB-CASE" => Some(ScreamingKebabCase),
            "Train-Case" => Some(TrainCase),
            _ => None,
        }
    }
    /// Apply a renaming rule to an enum or struct field, returning the version expected in the source.
    pub fn apply_to_field(&self, variant: &str) -> String {
        match *self {
            LowerCase => variant.to_lowercase(),
            UpperCase => variant.to_uppercase(),
            PascalCase => variant.to_upper_camel_case(),
            CamelCase => variant.to_lower_camel_case(),
            SnakeCase => variant.to_snake_case(),
            ScreamingSnakeCase => variant.to_shouty_snake_case(),
            KebabCase => variant.to_kebab_case(),
            ScreamingKebabCase => variant.to_shouty_kebab_case(),
            TrainCase => variant.to_train_case(),
        }
    }
}

#[test]
fn rename_field() {
    for &(original, lower, upper, camel, snake, screaming, kebab, screaming_kebab) in &[
        (
            "Outcome", "outcome", "OUTCOME", "outcome", "outcome", "OUTCOME", "outcome", "OUTCOME",
        ),
        (
            "VeryTasty",
            "verytasty",
            "VERYTASTY",
            "veryTasty",
            "very_tasty",
            "VERY_TASTY",
            "very-tasty",
            "VERY-TASTY",
        ),
        ("A", "a", "A", "a", "a", "A", "a", "A"),
        ("Z42", "z42", "Z42", "z42", "z42", "Z42", "z42", "Z42"),
    ] {
        assert_eq!(LowerCase.apply_to_field(original), lower);
        assert_eq!(UpperCase.apply_to_field(original), upper);
        assert_eq!(PascalCase.apply_to_field(original), original);
        assert_eq!(CamelCase.apply_to_field(original), camel);
        assert_eq!(SnakeCase.apply_to_field(original), snake);
        assert_eq!(ScreamingSnakeCase.apply_to_field(original), screaming);
        assert_eq!(KebabCase.apply_to_field(original), kebab);
        assert_eq!(ScreamingKebabCase.apply_to_field(original), screaming_kebab);
    }
}
