# Rust-openGauss

openGauss support for Rust.

## opengauss [![Latest Version](https://img.shields.io/crates/v/opengauss.svg)](https://crates.io/crates/opengauss)

[Documentation](https://docs.rs/opengauss)

A native, synchronous openGauss client.

## tokio-opengauss [![Latest Version](https://img.shields.io/crates/v/tokio-opengauss.svg)](https://crates.io/crates/tokio-opengauss)

[Documentation](https://docs.rs/tokio-opengauss)

A native, asynchronous openGauss client.

## opengauss-types [![Latest Version](https://img.shields.io/crates/v/opengauss-types.svg)](https://crates.io/crates/opengauss-types)

[Documentation](https://docs.rs/opengauss-types)

Conversions between Rust and openGauss types.

## opengauss-native-tls [![Latest Version](https://img.shields.io/crates/v/opengauss-native-tls.svg)](https://crates.io/crates/opengauss-native-tls)

[Documentation](https://docs.rs/opengauss-native-tls)

TLS support for opengauss and tokio-opengauss via native-tls.

## opengauss-openssl [![Latest Version](https://img.shields.io/crates/v/opengauss-openssl.svg)](https://crates.io/crates/opengauss-openssl)

[Documentation](https://docs.rs/opengauss-openssl)

TLS support for opengauss and tokio-opengauss via openssl.

# Running test suite

The test suite requires openGauss to be running in the correct configuration. The easiest way to do this is with docker:

1. Install `docker` and `docker-compose`.
   1. On ubuntu: `sudo apt install docker.io docker-compose`.
1. Make sure your user has permissions for docker.
   1. On ubuntu: ``sudo usermod -aG docker $USER``
1. Change to top-level directory of `rust-opengauss` repo.
1. Run `docker-compose up -d`.
1. Run `cargo test`.
1. Run `docker-compose stop`.
