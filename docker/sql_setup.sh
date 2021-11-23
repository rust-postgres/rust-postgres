#!/bin/bash
set -e

cat > "$PGDATA/server.key" <<-EOKEY
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAllItXwrj62MkxKVlz2FimJk42WWc3K82Rn2vAl6z38zQxSCj
t9uWwXWTx5YOdGiUcA+JUAruZxqN7vdfphJoYtTrcrpT4rC/FsCMImBxkj1cxdYT
q94SFn9bQBRZk7RUx4Kolt+/h0d3PpNIb4DbyQ8A0MVvNVxLpRRVwc6yQP+NkRMy
gHR+m3P8fxHEtkHCVy7HORbASvN8fRlREMHDL2hkadX0BNM72DDo+DWhPA8GF6WX
tIl1gU6GP6pSbEeMHD3f+uj7f9iSjvkrHrOt2nLUQ9Qnev2nhmU0/dOIweQ17/Fr
lL9jYDUUFNORyjRnlXXUoP5BO/LdEAAqT2A0pwIDAQABAoIBAQCIXu74XUneHuiZ
Wa+eTqwC4mZXmz6OWonzs0vU65NlgksXuv+r6ZO/2GoD1Bcy9jlL3Fxm+DPF56pB
07u7TtHSb3VWdMFrU4tYGcBH45TE5dRHSmo4LlPcgxeGb6/ANwX+pYNKtJvuHyCH
7Vf2iEFcCrdjrumv0BZ0IZmXJGxEV+7mK2Og0bZ/zbmJNaH25muuWj6BKlvLhL0N
S2LlBjKx3HqtppUgUqNFqjLs6IA1u79S5dAomOsxZtnuByaX5WFzpktU2pveZmyF
cl0dwHYZIaxR3ewYeQXGF8ANUmIx3nnxD2JOysPkitaGzeqt6dQZV14tPlDZDKat
Vf0b6BHhAoGBAMWV7rG+7nVXoQ30CIcPGklkST3mVOlrzeBbKP1SeAwoGRbfsdhp
rFMkh5UxTexnOzD4O8HPuJ6NGeWRQfqZT1nnjwHPeJWtiMHT6cnWxlzvxAZ61mio
0jRfb8flhgFKk+G9+Xa6WaYAAwGWdF062EMe2Ym92oKM9ilTPGFVRk1XAoGBAMLD
ETSQd2UqTF/y7wxMPqF3l6d1KBjwpuNuin2IjkXTOfGkDnAU3mSQlr7K1IPX8NPO
gdyMfJoysfRaBuRcNA/o/0l0wyxW4HWtTtPYI0+pRCFtRLsI1MB997QKeaGKb+me
3nBXkOksPSr9oa0Cs27z2cSoBOkpq2N/zzBseHExAoGAOyq3rKBZNehEwTHnb9I0
8+9FA3U6zh9LKjkCIEGW00Uapj/cOMsEIG2a8DEwfW84SWS8OEBkr43fSGBkGo/Y
NDrkFw2ytVee0TQNGTTod6IQ2EPmera7I5XEml5/71kOyZWi40vQVqZAQDR2qgha
BFdzmwywJ1Hg0OUs+pSXlccCgYEAgyOVki80NYolovWQwFcWVOKR2s+oECL6PGlS
FvS714hCm9I7ZnymwlAZMJ6iOaRNJFEIX9i4jZtU95Mm0NzEsXHRc0SLpm9Y8+Oe
EEaYgCsZFOjePpHTr0kiYLgs7fipIkU2wa40hMyk4y2kjzoiV7MaDrCTnevQ205T
0+c1sgECgYBAXKcwdkh9JVSrLXFamsxiOx3MZ0n6J1d28wpdA3y4Y4AAJm4TGgFt
eG/6qHRy6CHdFtJ7a84EMe1jaVLQJYW/VrOC2bWLftkU7qaOnkXHvr4CAHsXQHcx
JhLfvh4ab3KyoK/iimifvcoS5z9gp7IBFKMyh5IeJ9Y75TgcfJ5HMg==
-----END RSA PRIVATE KEY-----
EOKEY
chmod 0600 "$PGDATA/server.key"

cat > "$PGDATA/server.crt" <<-EOCERT
-----BEGIN CERTIFICATE-----
MIID9DCCAtygAwIBAgIJAIYfg4EQ2pVAMA0GCSqGSIb3DQEBBQUAMFkxCzAJBgNV
BAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX
aWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xNjA2MjgyMjQw
NDFaFw0yNjA2MjYyMjQwNDFaMFkxCzAJBgNVBAYTAkFVMRMwEQYDVQQIEwpTb21l
LVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNV
BAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAJZS
LV8K4+tjJMSlZc9hYpiZONllnNyvNkZ9rwJes9/M0MUgo7fblsF1k8eWDnRolHAP
iVAK7mcaje73X6YSaGLU63K6U+KwvxbAjCJgcZI9XMXWE6veEhZ/W0AUWZO0VMeC
qJbfv4dHdz6TSG+A28kPANDFbzVcS6UUVcHOskD/jZETMoB0fptz/H8RxLZBwlcu
xzkWwErzfH0ZURDBwy9oZGnV9ATTO9gw6Pg1oTwPBhell7SJdYFOhj+qUmxHjBw9
3/ro+3/Yko75Kx6zrdpy1EPUJ3r9p4ZlNP3TiMHkNe/xa5S/Y2A1FBTTkco0Z5V1
1KD+QTvy3RAAKk9gNKcCAwEAAaOBvjCBuzAdBgNVHQ4EFgQUEcuoFxzUZ4VV9VPv
5frDyIuFA5cwgYsGA1UdIwSBgzCBgIAUEcuoFxzUZ4VV9VPv5frDyIuFA5ehXaRb
MFkxCzAJBgNVBAYTAkFVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJ
bnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdIIJAIYf
g4EQ2pVAMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAHwMzmXdtz3R
83HIdRQic40bJQf9ucSwY5ArkttPhC8ewQGyiGexm1Tvx9YA/qT2rscKPHXCPYcP
IUE+nJTc8lQb8wPnFwGdHUsJfCvurxE4Yv4Oi74+q1enhHBGsvhFdFY5jTYD9unM
zBEn+ZHX3PlKhe3wMub4khBTbPLK+n/laQWuZNsa+kj7BynkAg8W/6RK0Z0cJzzw
aiVP0bSvatAAcSwkEfKEv5xExjWqoewjSlQLEZYIjJhXdtx/8AMnrcyxrFvKALUQ
9M15FXvlPOB7ez14xIXQBKvvLwXvteHF6kYbzg/Bl1Q2GE9usclPa4UvTpnLv6gq
NmFaAhoxnXA=
-----END CERTIFICATE-----
EOCERT

cat >> "$PGDATA/postgresql.conf" <<-EOCONF
port = 5433
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
wal_level = logical
EOCONF

cat > "$PGDATA/pg_hba.conf" <<-EOCONF
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    all             pass_user       0.0.0.0/0            password
host    all             md5_user        0.0.0.0/0            md5
host    all             scram_user      0.0.0.0/0            scram-sha-256
host    all             pass_user       ::0/0                password
host    all             md5_user        ::0/0                md5
host    all             scram_user      ::0/0                scram-sha-256

hostssl all             ssl_user        0.0.0.0/0            trust
hostssl all             ssl_user        ::0/0                trust
host    all             ssl_user        0.0.0.0/0            reject
host    all             ssl_user        ::0/0                reject

# IPv4 local connections:
host    all             postgres        0.0.0.0/0            trust
host    replication     postgres        0.0.0.0/0            trust
# IPv6 local connections:
host    all             postgres        ::0/0                trust
# Unix socket connections:
local   all             postgres                             trust
EOCONF

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE ROLE pass_user PASSWORD 'password' LOGIN;
    CREATE ROLE md5_user PASSWORD 'password' LOGIN;
    SET password_encryption TO 'scram-sha-256';
    CREATE ROLE scram_user PASSWORD 'password' LOGIN;
    CREATE ROLE ssl_user LOGIN;
    CREATE EXTENSION hstore;
    CREATE EXTENSION citext;
    CREATE EXTENSION ltree;
EOSQL
