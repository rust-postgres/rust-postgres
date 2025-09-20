//! A synchronous client for the PostgreSQL database.
//!
//! # Example
//!
//! ```no_run
//! use postgres::{Client, NoTls};
//!
//! # fn main() -> Result<(), postgres::Error> {
//! let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
//!
//! client.batch_execute("
//!     CREATE TABLE person (
//!         id      SERIAL PRIMARY KEY,
//!         name    TEXT NOT NULL,
//!         data    BYTEA
//!     )
//! ")?;
//!
//! let name = "Ferris";
//! let data = None::<&[u8]>;
//! client.execute(
//!     "INSERT INTO person (name, data) VALUES ($1, $2)",
//!     &[&name, &data],
//! )?;
//!
//! for row in client.query("SELECT id, name, data FROM person", &[])? {
//!     let id: i32 = row.get(0);
//!     let name: &str = row.get(1);
//!     let data: Option<&[u8]> = row.get(2);
//!
//!     println!("found person: {} {} {:?}", id, name, data);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Implementation
//!
//! This crate is a lightweight wrapper over tokio-postgres. The `postgres::Client` is simply a wrapper around a
//! `tokio_postgres::Client` along side a tokio `Runtime`. The client simply blocks on the futures provided by the async
//! client.
//!
//! # SSL/TLS support
//!
//! TLS support is implemented via external libraries. `Client::connect` and `Config::connect` take a TLS implementation
//! as an argument. The `NoTls` type in this crate can be used when TLS is not required. Otherwise, the
//! `postgres-openssl` and `postgres-native-tls` crates provide implementations backed by the `openssl` and `native-tls`
//! crates, respectively.
//!
//! # Features
//!
//! The following features can be enabled from `Cargo.toml`:
//!
//! | Feature | Description | Extra dependencies | Default |
//! | ------- | ----------- | ------------------ | ------- |
//! | `with-bit-vec-0_6` | Enable support for the `bit-vec` crate. | [bit-vec](https://crates.io/crates/bit-vec) 0.6 | no |
//! | `with-bit-vec-0_7` | Enable support for the `bit-vec` crate. | [bit-vec](https://crates.io/crates/bit-vec) 0.7 | no |
//! | `with-bit-vec-0_8` | Enable support for the `bit-vec` crate. | [bit-vec](https://crates.io/crates/bit-vec) 0.8 | no |
//! | `with-chrono-0_4` | Enable support for the `chrono` crate. | [chrono](https://crates.io/crates/chrono) 0.4 | no |
//! | `with-eui48-0_4` | Enable support for the 0.4 version of the `eui48` crate. This is deprecated and will be removed. | [eui48](https://crates.io/crates/eui48) 0.4 | no |
//! | `with-eui48-1` | Enable support for the 1.0 version of the `eui48` crate. | [eui48](https://crates.io/crates/eui48) 1.0 | no |
//! | `with-geo-types-0_6` | Enable support for the 0.6 version of the `geo-types` crate. | [geo-types](https://crates.io/crates/geo-types/0.6.0) 0.6 | no |
//! | `with-geo-types-0_7` | Enable support for the 0.7 version of the `geo-types` crate. | [geo-types](https://crates.io/crates/geo-types/0.7.0) 0.7 | no |
//! | `with-serde_json-1` | Enable support for the `serde_json` crate. | [serde_json](https://crates.io/crates/serde_json) 1.0 | no |
//! | `with-uuid-0_8` | Enable support for the `uuid` crate. | [uuid](https://crates.io/crates/uuid) 0.8 | no |
//! | `with-uuid-1` | Enable support for the `uuid` crate. | [uuid](https://crates.io/crates/uuid) 1.0 | no |
//! | `with-time-0_2` | Enable support for the 0.2 version of the `time` crate. | [time](https://crates.io/crates/time/0.2.0) 0.2 | no |
//! | `with-time-0_3` | Enable support for the 0.3 version of the `time` crate. | [time](https://crates.io/crates/time/0.3.0) 0.3 | no |
#![warn(clippy::all, rust_2018_idioms, missing_docs)]

pub use fallible_iterator;
pub use tokio_postgres::{
    error, row, tls, types, Column, IsolationLevel, Notification, Portal, SimpleQueryMessage,
    Socket, Statement, ToStatement,
};

pub use crate::cancel_token::CancelToken;
pub use crate::client::*;
pub use crate::config::Config;
pub use crate::copy_in_writer::CopyInWriter;
pub use crate::copy_out_reader::CopyOutReader;
#[doc(no_inline)]
pub use crate::error::Error;
pub use crate::generic_client::GenericClient;
#[doc(inline)]
pub use crate::notifications::Notifications;
#[doc(no_inline)]
pub use crate::row::{Row, SimpleQueryRow};
pub use crate::row_iter::RowIter;
#[doc(no_inline)]
pub use crate::tls::NoTls;
pub use crate::transaction::*;
pub use crate::transaction_builder::TransactionBuilder;

pub mod binary_copy;
mod cancel_token;
mod client;
pub mod config;
mod connection;
mod copy_in_writer;
mod copy_out_reader;
mod generic_client;
mod lazy_pin;
pub mod notifications;
mod row_iter;
mod transaction;
mod transaction_builder;

#[cfg(test)]
mod test;
