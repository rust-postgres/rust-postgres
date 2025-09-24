# Change Log

## v0.4.7 - 2025-09-25

### Added

* Added support for nested domains containing composite types to `FromSql`

### Fixed

* Added `dyn` keyword to boxed trait objects.

### Changed

* Updated repository links to use `rust-postgres` organization.
* Upgraded to Rust 2021 edition.

## v0.4.6 - 2024-09-15

### Changed

* Upgraded `heck`.

## v0.4.5 - 2023-08-19

### Added

* Added a `rename_all` option for enum and struct derives.
* Added an `allow_mismatch` option to disable strict enum variant checks against the Postgres type.

## v0.4.4 - 2023-03-27

### Changed

* Upgraded `syn`.

## v0.4.3 - 2022-09-07

### Added

* Added support for parameterized structs.

## v0.4.2 - 2022-04-30

### Added

* Added support for transparent wrapper types.

## v0.4.1 - 2021-11-23

### Fixed

* Fixed handling of struct fields using raw identifiers.

## v0.4.0 - 2019-12-23

No changes

## v0.4.0-alpha.1 - 2019-10-14

* Initial release
