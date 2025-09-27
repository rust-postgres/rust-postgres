# Change Log

## v0.7.14 - 2025-09-25

### Added

* Added support for `jiff` v0.2.
* Added support for `bit-vec` versions v0.7 and v0.8.
* Re-exported `fallible-iterator`
* Expose sync rollback as a private API method

### Fixes

* Changed `set_tcp_user_timeout` to only be called when enabled

### Changed

* Updated repository links to use `rust-postgres` organization.
* Upgraded to Rust 2021 edition.
* Upgraded `phf` to v0.13
* Upgraded `socket2` to v0.6
* Disabled default features of `futures-util`

## v0.7.13 - 2025-02-02

### Added

* Added support for direct TLS negotiation.
* Added support for `cidr` 0.3 via the `with-cidr-0_3` feature.

### Fixes

* Added `load_balance_hosts` to `Config`'s `Debug` implementation.

### Changes

* Upgraded `rand`.

## v0.7.12 - 2024-09-15

### Fixed

* Fixed `query_typed` queries that return no rows.

### Added

* Added support for `jiff` 0.1 via the `with-jiff-01` feature.
* Added support for TCP keepalive on AIX.

## v0.7.11 - 2024-07-21

### Fixed

* Fixed handling of non-UTF8 error fields which can be sent after failed handshakes.
* Fixed cancellation handling of `TransactionBuilder::start` futures.

### Added

* Added `table_oid` and `field_id` fields to `Columns` struct of prepared statements.
* Added `GenericClient::simple_query`.
* Added `#[track_caller]` to `Row::get` and `SimpleQueryRow::get`.
* Added `TargetSessionAttrs::ReadOnly`.
* Added `Debug` implementation for `Statement`.
* Added `Clone` implementation for `Row`.
* Added `SimpleQueryMessage::RowDescription`.
* Added `{Client, Transaction, GenericClient}::query_typed`.

### Changed

* Disable `rustc-serialize` compatibility of `eui48-1` dependency
* Config setters now take `impl Into<String>`.

## v0.7.10 - 2023-08-25

## Fixed

* Defered default username lookup to avoid regressing `Config` behavior.

## v0.7.9 - 2023-08-19

## Fixed

* Fixed builds on OpenBSD.

## Added

* Added the `js` feature for WASM support.
* Added support for the `hostaddr` config option to bypass DNS lookups.
* Added support for the `load_balance_hosts` config option to randomize connection ordering.
* The `user` config option now defaults to the executing process's user.

## v0.7.8 - 2023-05-27

## Added

* Added `keepalives_interval` and `keepalives_retries` config options.
* Added new `SqlState` variants.
* Added more `Debug` impls.
* Added `GenericClient::batch_execute`.
* Added `RowStream::rows_affected`.
* Added the `tcp_user_timeout` config option.

## Changed

* Passing an incorrect number of parameters to a query method now returns an error instead of panicking.
* Upgraded `socket2`.

## v0.7.7 - 2022-08-21

## Added

* Added `ToSql` and `FromSql` implementations for `[u8; N]` via the `array-impls` feature.
* Added support for `smol_str` 0.1 via the `with-smol_str-01` feature.
* Added `ToSql::encode_format` to support text encodings of parameters.

## v0.7.6 - 2022-04-30

### Added

* Added support for `uuid` 1.0 via the `with-uuid-1` feature.

### Changed

* Upgraded to `tokio-util` 0.7.
* Upgraded to `parking_lot` 0.12.

## v0.7.5 - 2021-10-29

### Fixed

* Fixed a bug where the client could enter into a transaction if the `Client::transaction` future was dropped before completion.

## v0.7.4 - 2021-10-19

### Fixed

* Fixed reporting of commit-time errors triggered by deferred constraints.

## v0.7.3 - 2021-09-29

### Fixed

* Fixed a deadlock when pipelined requests concurrently prepare cached typeinfo queries.

### Added

* Added `SimpleQueryRow::columns`.
* Added support for `eui48` 1.0 via the `with-eui48-1` feature.
* Added `FromSql` and `ToSql` implementations for arrays via the `array-impls` feature.
* Added support for `time` 0.3 via the `with-time-0_3` feature.

## v0.7.2 - 2021-04-25

### Fixed

* `SqlState` constants can now be used in `match` patterns.

## v0.7.1 - 2021-04-03

### Added

* Added support for `geo-types` 0.7 via `with-geo-types-0_7` feature.
* Added `Client::clear_type_cache`.
* Added `Error::as_db_error` and `Error::is_closed`.

## v0.7.0 - 2020-12-25

### Changed

* Upgraded to `tokio` 1.0.
* Upgraded to `postgres-types` 0.2.

### Added

* Methods taking iterators of `ToSql` values can now take both `&dyn ToSql` and `T: ToSql` values.

## v0.6.0 - 2020-10-17

### Changed

* Upgraded to `tokio` 0.3.
* Added the detail and hint fields to `DbError`'s `Display` implementation.

## v0.5.5 - 2020-07-03

### Added

* Added support for `geo-types` 0.6.

## v0.5.4 - 2020-05-01

### Added

* Added `Transaction::savepoint`, which can be used to create a savepoint with a custom name.

## v0.5.3 - 2020-03-05

### Added

* Added `Debug` implementations for `Client`, `Row`, and `Column`.
* Added `time` 0.2 support.

## v0.5.2 - 2020-01-31

### Fixed

* Notice messages sent during the initial connection process are now collected and returned first from
    `Connection::poll_message`.

### Deprecated

* Deprecated `Client::cancel_query` and `Client::cancel_query_raw` in favor of `Client::cancel_token`.

### Added

* Added `Client::build_transaction` to allow configuration of various transaction options.
* Added `Client::cancel_token`, which returns a separate owned object that can be used to cancel queries.
* Added accessors for `Config` fields.
* Added a `GenericClient` trait implemented for `Client` and `Transaction` and covering shared functionality.

## v0.5.1 - 2019-12-25

### Fixed

* Removed some stray `println!`s from `copy_out` internals.

## v0.5.0 - 2019-12-23

### Changed

* `Client::copy_in` now returns a `Sink` rather than taking in a `Stream`.
* `CopyStream` has been renamed to `CopyOutStream`.
* `Client::copy_in` and `Client::copy_out` no longer take query parameters as PostgreSQL doesn't support parameters in
    COPY queries.
* `TargetSessionAttrs`, `SslMode`, and `ChannelBinding` are now true non-exhaustive enums.

### Added

* Added `Client::query_opt` for queries expected to return zero or one rows.
* Added binary copy format support to the `binary_copy` module.
* Added back query logging.

### Removed

* Removed `uuid` 0.7 support.

## v0.5.0-alpha.2 - 2019-11-27

### Changed

* Upgraded `bytes` to 0.5.
* Upgraded `tokio` to 0.2.
* The TLS interface uses a trait to obtain channel binding information rather than returning it after the handshake.
* Changed the value of the `timezone` property from `GMT` to `UTC`.
* Returned `Stream` implementations are now `!Unpin`.

### Added

* Added support for `uuid` 0.8.
* Added the column to `Row::try_get` errors.

## v0.5.0-alpha.1 - 2019-10-14

### Changed

* The library now uses `std::futures::Future` and async/await syntax.
* Most methods now take `&self` rather than `&mut self`.
* The transaction API has changed to more closely resemble the synchronous API and is significantly more ergonomic.
* Methods now take `&[&(dyn ToSql + Sync)]` rather than `&[&dyn ToSql]` to allow futures to be `Send`.
* Methods are now "normal" async functions that no longer do work up-front.
* Statements are no longer required to be prepared explicitly before use. Methods taking `&Statement` can now also take
    `&str`, and will internally prepare the statement.
* `ToSql` now serializes its value into a `BytesMut` rather than `Vec<u8>`.
* Methods that previously returned `Stream`s now return `Vec<T>`. New `*_raw` methods still provide a `Stream`
    interface.

### Added

* Added the `channel_binding=disable/allow/require` configuration to control use of channel binding.
* Added the `Client::query_one` method to cover the common case of a query that returns exactly one row.

## v0.4.0-rc.3 - 2019-06-29

### Fixed

* Significantly improved the performance of `query` and `copy_in`.

### Changed

* The items of the stream passed to `copy_in` must be `'static`.

## v0.4.0-rc.2 - 2019-03-05

### Fixed

* Fixed Cargo features to actually enable the functionality they claim to.

## v0.4.0-rc.1 - 2019-03-05

### Changed

* The client API has been significantly overhauled. It now resembles `hyper`'s, with separate `Connection` and `Client`
    objects. See the crate-level documentation for more details.
* The TLS connection mode (e.g. `prefer`) is now part of the connection configuration rather than being passed in
    separately.
* The Cargo features enabling `ToSql` and `FromSql` implementations for external crates are now versioned. For example,
    `with-uuid` is now `with-uuid-0_7`. This enables us to add support for new major versions of the crates in parallel
    without breaking backwards compatibility.
* Upgraded from `tokio-core` to `tokio`.

### Added

* Connection string configuration now more fully mirrors libpq's syntax, and supports both URL-style and key-value style
    strings.
* `FromSql` implementations can now borrow from the data buffer. In particular, this means that you can deserialize
    values as `&str`. The `FromSqlOwned` trait can be used as a bound to restrict code to deserializing owned values.
* Added support for channel binding with SCRAM authentication.
* Added multi-host support in connection configuration.
* The client now supports query pipelining, which can be used as a latency hiding measure.
* While the crate uses `tokio` by default, the base API can be used with any asynchronous stream type on any reactor.
* Added support for simple query requests returning row data.

### Removed

* The `with-openssl` feature has been removed. Use the `tokio-postgres-openssl` crate instead.
* The `with-rustc_serialize` and `with-time` features have been removed. Use `serde` and `SystemTime` or `chrono`
    instead.

## Older

Look at the [release tags] for information about older releases.

[release tags]: https://github.com/rust-postgres/rust-postgres/releases
