use heck::{
    ToKebabCase, ToLowerCamelCase, ToShoutyKebabCase, ToShoutySnakeCase, ToSnakeCase, ToTrainCase,
    ToUpperCamelCase,
};

use self::RenameRule::*;

#[allow(clippy::enum_variant_names)]
#[derive(Copy, Clone)]
pub enum RenameRule {
    LowerCase,
    UpperCase,
    PascalCase,
    CamelCase,
    SnakeCase,
    ScreamingSnakeCase,
    KebabCase,
    ScreamingKebabCase,
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

    pub fn apply_to_field(&self, field: &str) -> String {
        match *self {
            LowerCase => field.to_lowercase(),
            UpperCase => field.to_uppercase(),
            PascalCase => field.to_upper_camel_case(),
            CamelCase => field.to_lower_camel_case(),
            SnakeCase => field.to_snake_case(),
            ScreamingSnakeCase => field.to_shouty_snake_case(),
            KebabCase => field.to_kebab_case(),
            ScreamingKebabCase => field.to_shouty_kebab_case(),
            TrainCase => field.to_train_case(),
        }
    }
}
