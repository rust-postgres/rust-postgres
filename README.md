# Rust-Postgres

PostgreSQL support for Rust.

## postgres [![Latest Version](https://img.shields.io/crates/v/postgres.svg)](https://crates.io/crates/postgres)

[Documentation](https://docs.rs/postgres)

A native, synchronous PostgreSQL client.

## tokio-postgres [![Latest Version](https://img.shields.io/crates/v/tokio-postgres.svg)](https://crates.io/crates/tokio-postgres)

[Documentation](https://docs.rs/tokio-postgres)

A native, asynchronous PostgreSQL client.

## postgres-types [![Latest Version](https://img.shields.io/crates/v/postgres-types.svg)](https://crates.io/crates/postgres-types)

[Documentation](https://docs.rs/postgres-types)

Conversions between Rust and Postgres types.

## postgres-native-tls [![Latest Version](https://img.shields.io/crates/v/postgres-native-tls.svg)](https://crates.io/crates/postgres-native-tls)

[Documentation](https://docs.rs/postgres-native-tls)

TLS support for postgres and tokio-postgres via native-tls.

## postgres-openssl [![Latest Version](https://img.shields.io/crates/v/postgres-openssl.svg)](https://crates.io/crates/postgres-openssl)

[Documentation](https://docs.rs/postgres-openssl)

TLS support for postgres and tokio-postgres via openssl.

## Related crates

[`pg_named_args`](https://crates.io/crates/pg_named_args) enables one to use
named arguments in queries. The provided macro converts the named arguments to
positional arguments at compile time.

# Running test suite

The test suite requires postgres to be running in the correct configuration. The easiest way to do this is with docker:

1. Install `docker` and `docker-compose`.
   1. On ubuntu: `sudo apt install docker.io docker-compose`.
1. Make sure your user has permissions for docker.
   1. On ubuntu: ``sudo usermod -aG docker $USER``
1. Change to top-level directory of `rust-postgres` repo.
1. Run `docker-compose up -d`.
1. Run `cargo test`.
1. Run `docker-compose stop`.
