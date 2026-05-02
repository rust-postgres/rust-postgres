use bytes::BytesMut;
use cidr_03::{IpCidr, IpInet};
use opengauss_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for IpCidr {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let inet = types::inet_from_sql(raw)?;
        Ok(IpCidr::new(inet.addr(), inet.netmask())?)
    }

    accepts!(CIDR);
}

impl ToSql for IpCidr {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::inet_to_sql(self.first_address(), self.network_length(), w);
        Ok(IsNull::No)
    }

    accepts!(CIDR);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for IpInet {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let inet = types::inet_from_sql(raw)?;
        Ok(IpInet::new(inet.addr(), inet.netmask())?)
    }

    accepts!(INET);
}

impl ToSql for IpInet {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::inet_to_sql(self.address(), self.network_length(), w);
        Ok(IsNull::No)
    }

    accepts!(INET);
    to_sql_checked!();
}
