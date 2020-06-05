use super::write_str;
use crate::formatter::Formatter;
use crate::types::{EncodingType, RdbResult};
use std::io;
use std::io::Write;

pub struct Protocol {
    out: Box<dyn Write + 'static>,
    last_expiry: Option<u64>,
}

impl Protocol {
    pub fn new() -> Protocol {
        let out = Box::new(io::stdout());
        Protocol {
            out: out,
            last_expiry: None,
        }
    }
}

impl Protocol {
    fn emit(&mut self, args: Vec<&[u8]>) -> RdbResult<()> {
        write_str(&mut self.out, "*")?;
        self.out.write_all(args.len().to_string().as_bytes())?;
        write_str(&mut self.out, "\r\n")?;
        for arg in &args {
            write_str(&mut self.out, "$")?;
            self.out.write_all(arg.len().to_string().as_bytes())?;
            write_str(&mut self.out, "\r\n")?;
            self.out.write_all(arg)?;
            write_str(&mut self.out, "\r\n")?;
        }

        Ok(())
    }

    fn pre_expire(&mut self, expiry: Option<u64>) {
        self.last_expiry = expiry
    }

    fn post_expire(&mut self, key: &[u8]) -> RdbResult<()> {
        if let Some(expire) = self.last_expiry {
            let expire = expire.to_string();
            self.emit(vec!["PEXPIREAT".as_bytes(), key, expire.as_bytes()])?;
            self.last_expiry = None;
        }

        Ok(())
    }
}

impl Formatter for Protocol {
    fn start_rdb(&mut self) -> RdbResult<()> {
        Ok(())
    }

    fn end_rdb(&mut self) -> RdbResult<()> {
        Ok(())
    }

    fn start_database(&mut self, db_number: u32) -> RdbResult<()> {
        let db = db_number.to_string();
        self.emit(vec!["SELECT".as_bytes(), db.as_bytes()])?;

        Ok(())
    }

    fn set(&mut self, key: &[u8], value: &[u8], expiry: Option<u64>) -> RdbResult<()> {
        self.pre_expire(expiry);
        self.emit(vec!["SET".as_bytes(), key, value])?;
        self.post_expire(key)?;
        Ok(())
    }

    fn start_hash(
        &mut self,
        _key: &[u8],
        _length: u32,
        expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.pre_expire(expiry);

        Ok(())
    }

    fn end_hash(&mut self, key: &[u8]) -> RdbResult<()> {
        self.post_expire(key)?;

        Ok(())
    }
    fn hash_element(&mut self, key: &[u8], field: &[u8], value: &[u8]) -> RdbResult<()> {
        self.emit(vec!["HSET".as_bytes(), key, field, value])?;
        Ok(())
    }

    fn start_set(
        &mut self,
        _key: &[u8],
        _cardinality: u32,
        expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.pre_expire(expiry);
        Ok(())
    }
    fn end_set(&mut self, key: &[u8]) -> RdbResult<()> {
        self.post_expire(key)?;
        Ok(())
    }
    fn set_element(&mut self, key: &[u8], member: &[u8]) -> RdbResult<()> {
        self.emit(vec!["SADD".as_bytes(), key, member])?;
        Ok(())
    }

    fn start_list(
        &mut self,
        _key: &[u8],
        _length: u32,
        expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.pre_expire(expiry);
        Ok(())
    }
    fn end_list(&mut self, key: &[u8]) -> RdbResult<()> {
        self.post_expire(key)?;
        Ok(())
    }
    fn list_element(&mut self, key: &[u8], value: &[u8]) -> RdbResult<()> {
        self.emit(vec!["RPUSH".as_bytes(), key, value])?;
        Ok(())
    }

    fn start_sorted_set(
        &mut self,
        _key: &[u8],
        _length: u32,
        expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.pre_expire(expiry);
        Ok(())
    }

    fn end_sorted_set(&mut self, key: &[u8]) -> RdbResult<()> {
        self.post_expire(key)?;
        Ok(())
    }

    fn sorted_set_element(&mut self, key: &[u8], score: f64, member: &[u8]) -> RdbResult<()> {
        let score = score.to_string();
        self.emit(vec!["ZADD".as_bytes(), key, score.as_bytes(), member])?;
        Ok(())
    }
}
