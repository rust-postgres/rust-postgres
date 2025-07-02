use std::time::Duration;
use tokio_postgres::config::{Config, SslNegotiation, TargetSessionAttrs};

fn check(s: &str, config: &Config) {
    assert_eq!(s.parse::<Config>().expect(s), *config, "`{s}`");
}

#[test]
fn pairs_ok() {
    check(
        r"user=foo password=' fizz \'buzz\\ ' application_name = ''",
        Config::new()
            .user("foo")
            .password(r" fizz 'buzz\ ")
            .application_name(""),
    );
}

#[test]
fn pairs_ws() {
    check(
        " user\t=\r\n\x0bfoo \t password = hunter2 ",
        Config::new().user("foo").password("hunter2"),
    );
}

#[test]
fn settings() {
    check(
        "connect_timeout=3 keepalives=0 keepalives_idle=30 target_session_attrs=read-write",
        Config::new()
            .connect_timeout(Duration::from_secs(3))
            .keepalives(false)
            .keepalives_idle(Duration::from_secs(30))
            .target_session_attrs(TargetSessionAttrs::ReadWrite),
    );
    check(
        "connect_timeout=3 keepalives=0 keepalives_idle=30 target_session_attrs=read-only",
        Config::new()
            .connect_timeout(Duration::from_secs(3))
            .keepalives(false)
            .keepalives_idle(Duration::from_secs(30))
            .target_session_attrs(TargetSessionAttrs::ReadOnly),
    );
    check(
        "sslnegotiation=direct",
        Config::new().ssl_negotiation(SslNegotiation::Direct),
    );
}

#[test]
fn keepalive_settings() {
    check(
        "keepalives=1 keepalives_idle=15 keepalives_interval=5 keepalives_retries=9",
        Config::new()
            .keepalives(true)
            .keepalives_idle(Duration::from_secs(15))
            .keepalives_interval(Duration::from_secs(5))
            .keepalives_retries(9),
    );
}

#[test]
fn url() {
    check("postgresql://", &Config::new());
    check(
        "postgresql://localhost",
        Config::new().host("localhost").port(5432),
    );
    check(
        "postgresql://localhost:5433",
        Config::new().host("localhost").port(5433),
    );
    check(
        "postgresql://localhost/mydb",
        Config::new().host("localhost").port(5432).dbname("mydb"),
    );
    check(
        "postgresql://user@localhost",
        Config::new().user("user").host("localhost").port(5432),
    );
    check(
        "postgresql://user:secret@localhost",
        Config::new()
            .user("user")
            .password("secret")
            .host("localhost")
            .port(5432),
    );
    check(
        "postgresql://other@localhost/otherdb?connect_timeout=10&application_name=myapp",
        Config::new()
            .user("other")
            .host("localhost")
            .port(5432)
            .dbname("otherdb")
            .connect_timeout(Duration::from_secs(10))
            .application_name("myapp"),
    );
    check(
        "postgresql://host1:123,host2:456/somedb?target_session_attrs=any&application_name=myapp",
        Config::new()
            .host("host1")
            .port(123)
            .host("host2")
            .port(456)
            .dbname("somedb")
            .target_session_attrs(TargetSessionAttrs::Any)
            .application_name("myapp"),
    );
    check(
        "postgresql:///mydb?host=localhost&port=5433",
        Config::new().dbname("mydb").host("localhost").port(5433),
    );
    check(
        "postgresql://[2001:db8::1234]/database",
        Config::new()
            .host("2001:db8::1234")
            .port(5432)
            .dbname("database"),
    );
    check(
        "postgresql://[2001:db8::1234]:5433/database",
        Config::new()
            .host("2001:db8::1234")
            .port(5433)
            .dbname("database"),
    );
    #[cfg(unix)]
    check(
        "postgresql:///dbname?host=/var/lib/postgresql",
        Config::new()
            .dbname("dbname")
            .host_path("/var/lib/postgresql"),
    );
    #[cfg(unix)]
    check(
        "postgresql://%2Fvar%2Flib%2Fpostgresql/dbname",
        Config::new()
            .host_path("/var/lib/postgresql")
            .port(5432)
            .dbname("dbname"),
    )
}
