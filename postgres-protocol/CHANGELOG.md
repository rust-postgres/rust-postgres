# Change Log

## v0.6.9 - 2025-09-25

### Changed

* Updated repository links to use `rust-postgres` organization.
* Upgraded to Rust 2021 edition.

## v0.6.8 - 2025-02-02

### Changed

* Upgraded `getrandom`.

## v0.6.7 - 2024-07-21

### Deprecated

* Deprecated `ErrorField::value`.

### Added

* Added a `Clone` implementation for `DataRowBody`.
* Added `ErrorField::value_bytes`.

### Changed

* Upgraded `base64`.

## v0.6.6 - 2023-08-19

### Added

* Added the `js` feature for WASM support.

## v0.6.5 - 2023-03-27

### Added

* Added `message::frontend::flush`.
* Added `DataRowBody::buffer_bytes`.

### Changed

* Upgraded `base64`.

## v0.6.4 - 2022-04-03

### Added

* Added parsing support for `ltree`, `lquery`, and `ltxtquery`.

## v0.6.3 - 2021-12-10

### Changed

* Upgraded `hmac`, `md-5` and `sha`.

## v0.6.2 - 2021-09-29

### Changed

* Upgraded `hmac`.

## v0.6.1 - 2021-04-03

### Added

* Added the `password` module, which can be used to hash passwords before using them in queries like `ALTER USER`.
* Added type conversions for `LSN`.

### Changed

* Moved from `md5` to `md-5`.

## v0.6.0 - 2020-12-25

### Changed

* Upgraded `bytes`, `hmac`, and `rand`.

### Added

* Added `escape::{escape_literal, escape_identifier}`.

## v0.5.3 - 2020-10-17

### Changed

* Upgraded `base64` and `hmac`.

## v0.5.2 - 2020-07-06

### Changed

* Upgraded `hmac` and `sha2`.

## v0.5.1 - 2020-03-17

### Changed

* Upgraded `base64` to 0.12.

## v0.5.0 - 2019-12-23

### Changed

* `frontend::Message` is now a true non-exhaustive enum.

## v0.5.0-alpha.2 - 2019-11-27

### Changed

* Upgraded `bytes` to 0.5.

## v0.5.0-alpha.1 - 2019-10-14

### Changed

* Frontend messages and types now serialize to `BytesMut` rather than `Vec<u8>`.

## v0.4.1 - 2019-06-29

### Added

* Added `backend::Framed` to minimally parse the structure of backend messages.

## v0.4.0 - 2019-03-05

### Added

* Added channel binding support to SCRAM authentication API.

### Changed

* Passwords are no longer required to be UTF8 strings.
* `types::array_to_sql` now automatically computes the required flags and no longer takes a has_nulls parameter.

## Older

Look at the [release tags] for information about older releases.

[release tags]: https://github.com/rust-postgres/rust-postgres/releases
