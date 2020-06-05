use std::io::Write;

pub use self::json::JSON;
pub use self::nil::Nil;
pub use self::plain::Plain;
pub use self::protocol::Protocol;

use super::types::{EncodingType, RdbResult};

pub mod json;
pub mod nil;
pub mod plain;
pub mod protocol;

pub fn write_str<W: Write>(out: &mut W, data: &str) -> RdbResult<()> {
    out.write(data.as_bytes())?;

    Ok(())
}

#[allow(unused_variables)]
pub trait Formatter {
    fn start_rdb(&mut self) -> RdbResult<()> {
        Ok(())
    }
    fn end_rdb(&mut self) -> RdbResult<()> {
        Ok(())
    }
    fn checksum(&mut self, checksum: &[u8]) -> RdbResult<()> {
        Ok(())
    }

    fn start_database(&mut self, db_index: u32) -> RdbResult<()> {
        Ok(())
    }
    fn end_database(&mut self, db_index: u32) -> RdbResult<()> {
        Ok(())
    }

    fn resizedb(&mut self, db_size: u32, expires_size: u32) -> RdbResult<()> {
        Ok(())
    }
    fn aux_field(&mut self, key: &[u8], value: &[u8]) -> RdbResult<()> {
        Ok(())
    }

    fn set(&mut self, key: &[u8], value: &[u8], expiry: Option<u64>) -> RdbResult<()> {
        Ok(())
    }

    fn start_hash(
        &mut self,
        key: &[u8],
        length: u32,
        expiry: Option<u64>,
        info: EncodingType,
    ) -> RdbResult<()> {
        Ok(())
    }
    fn end_hash(&mut self, key: &[u8]) -> RdbResult<()> {
        Ok(())
    }
    fn hash_element(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> RdbResult<()> {
        Ok(())
    }

    fn start_set(
        &mut self,
        key: &[u8],
        cardinality: u32,
        expiry: Option<u64>,
        info: EncodingType,
    ) -> RdbResult<()> {
        Ok(())
    }
    fn end_set(&mut self, key: &[u8]) -> RdbResult<()> {
        Ok(())
    }
    fn set_element(&mut self, key: &[u8], member: &[u8]) -> RdbResult<()> {
        Ok(())
    }

    fn start_list(
        &mut self,
        key: &[u8],
        length: u32,
        expiry: Option<u64>,
        info: EncodingType,
    ) -> RdbResult<()> {
        Ok(())
    }
    fn end_list(&mut self, key: &[u8]) -> RdbResult<()> {
        Ok(())
    }
    fn list_element(&mut self, key: &[u8], value: &[u8]) -> RdbResult<()> {
        Ok(())
    }

    fn start_sorted_set(
        &mut self,
        key: &[u8],
        length: u32,
        expiry: Option<u64>,
        info: EncodingType,
    ) -> RdbResult<()> {
        Ok(())
    }
    fn end_sorted_set(&mut self, key: &[u8]) -> RdbResult<()> {
        Ok(())
    }
    fn sorted_set_element(&mut self, key: &[u8], score: f64, member: &[u8]) -> RdbResult<()> {
        Ok(())
    }
}
