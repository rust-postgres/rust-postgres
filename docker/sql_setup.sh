#!/bin/bash
set -e

cat > "$PGDATA/server.key" <<-EOKEY
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCgG5BsdsN/kNfD
MC6C1y+zf9RFthZDlcxI45+Baf9JEjpd5pAqvL7s2ZGGFgHUpWsKghVGwikvvTxG
keuxSFtnhcHqz1kebXw7PIWThnAYHHT6nEFm46ig3r69d6RitVy+ItdjHnlP9ErQ
1E7M2zOHxops0aPFAouKDD+KlyPLypSTSuCd+toLcFoLmlblK7vvcv9TcY59A348
sR8N/R+oiIF8mgFEZOHN2f3znvEyHaELwwzGS0R+biG4V1ZGCEmuoiX4Ay/7fMBh
YW1Y4yVKBFcoAifRDrIEx9ZITbmxMyOcaGTTshky8UaVaoZZUHLEnVwbcEx3pEu1
rTY1i/krAgMBAAECggEAAoiXy/CpcXF268xT69LrXx64lolILBxI3FkVOq47hL1L
xQRb7+8PWZYBZJWHPAVnbY6bjGJMCVnj6o47girwBTZJcfJQKaT3wG6eE9bAxkdN
AHPUz8f/Rg6NF762zATO9DrDDC44mo2xwFIXev93qY0JaZh77ic7W2F7CYcyvuEy
SvOirpSorKKdsepyntkBzrg9W9lQZ/6luso9J5jAZUAoFaQFa1P9xIkbpNcOf1fP
GDZ5XkU+QqhrHN5UzAeSiuuyavKRP44k9A5+93izkdxNtbFi4cPzAwqcf+eeKQD1
A6Qad+wHOtzvdX4TDTwObUzZVvG/WV66Aiwv1jBQoQKBgQDYyWHSv9PVt2EFvC3b
RDZ88Xo6P0wXGgP5W3xnyjgZTrxQMmcQqMMNwR225C+Fr12qLj1cGIB9BjuHdGBi
F/jjoNQhIbgDkkWfO3td0NLq9/wGlKBQzjsdHyNTFGoJtY+6O58uCyxCBmDm7xHT
P6/HMA9ARc1KUaLL3GXaLTjMlQKBgQC9EZSXGJJd3MJ2A45iCBGWWIE1jf1yUvj4
tYE4W8lfDTyO6ZrqL5drpnR7cDEuB1kRY1sKBPXUl+xSyga8bzQFh20e6PFWaTvP
LVdkW3Lw8xNp8+jn8L/W11o928jH/OjSSjHiYyA3SpsfZWRZJ2h40nqyTK3lZw6O
CvZsXRJ+vwKBgDWFYnVZjr8Qyw9Tai7cJGesZnTC89IwRhLmF0y4jCkTW0KhbviZ
8a4Po3pn06O5q/I8AEIgenhjdYb3oGQXbwcjybt0S632CuJJGSgMSkZgewRU5U+N
2uJRsbLtM1C6VoWv+pivgXm7gWkCVPBGpOsUXm+LzCcxCHQ0MaEv0PoRAoGBAKyf
LId3y8sfD/0n6gvfSg925yG6bji/QMnzDfQi+YxrTWs+Jk7C/QEwjRFWsdBQrSWP
DUPsm3Zq7z33bocFEP4rU5nxHMfEdPMHds8OH7eWd5c5NuDtknnZTW9FB+BwLTIy
w7DqyDMzTsYvkJtFu8D0i0nXcL4Ohd9yauMtZwGJAoGASUPgYv7j2fs8Y2KZG2FD
m0+PZPnKG3ZuX40d0IxFczPp/owwAoYAiudIzAVmF/nqBhw9tQkchH0dzS1hk3Op
KXTzOw5Q2IPlHRKY4jhldW0NneYMOk+ZaLfswjtPaORs7Zb8Y6xqgFwYs+Q1CqQO
q06h0QZ4bRUDrwWsG6NU5MU=
-----END PRIVATE KEY-----
EOKEY
chmod 0600 "$PGDATA/server.key"

cat > "$PGDATA/server.crt" <<-EOCERT
-----BEGIN CERTIFICATE-----
MIIDkzCCAnugAwIBAgIUTF4DePeZT01AB1mAluKLZDeRGtUwDQYJKoZIhvcNAQEL
BQAwWTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDESMBAGA1UEAwwJbG9jYWxob3N0MB4X
DTI2MDYwOTA4NDY1MVoXDTM2MDYwNjA4NDY1MVowWTELMAkGA1UEBhMCQVUxEzAR
BgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMgUHR5
IEx0ZDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEAoBuQbHbDf5DXwzAugtcvs3/URbYWQ5XMSOOfgWn/SRI6XeaQKry+
7NmRhhYB1KVrCoIVRsIpL708RpHrsUhbZ4XB6s9ZHm18OzyFk4ZwGBx0+pxBZuOo
oN6+vXekYrVcviLXYx55T/RK0NROzNszh8aKbNGjxQKLigw/ipcjy8qUk0rgnfra
C3BaC5pW5Su773L/U3GOfQN+PLEfDf0fqIiBfJoBRGThzdn9857xMh2hC8MMxktE
fm4huFdWRghJrqIl+AMv+3zAYWFtWOMlSgRXKAIn0Q6yBMfWSE25sTMjnGhk07IZ
MvFGlWqGWVByxJ1cG3BMd6RLta02NYv5KwIDAQABo1MwUTAdBgNVHQ4EFgQUpiCS
JHcnxUjr4GKKg0Icbmk4W14wHwYDVR0jBBgwFoAUpiCSJHcnxUjr4GKKg0Icbmk4
W14wDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAgZK5UPdkhgI8
BEeV7gm/ZmDiX3Dhp8l+PjagpDSpXkV0LmCEeA4jxskL5FdIr349lT4CZ7QWPkyv
OEmwG4N6qWU5H0fNmSHwBPVhQy6EknmCAhs0LsVXoy235ONHFbdgi3SVfJ5pMYgK
BVX/ADXErM4ifJzQoZFuQ6S8P3IstDDybeT+aPWOi5Fh2o7wYp9GqBFL/G+Nh9SI
xPs3x8oYzw/qDTXHduL1Onb8+ZTOSkMC9ONDUgmQUlIlpd/SCC1M8wzleyH5atMc
h/kmDuWEegS1ohJrZ1ATA5u66hyWU2eDFHi1NCIYwIuEfoULKihSqABDSuwe3wXS
RkTYLofsiw==
-----END CERTIFICATE-----
EOCERT

cat >> "$PGDATA/postgresql.conf" <<-EOCONF
port = 5433
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
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
