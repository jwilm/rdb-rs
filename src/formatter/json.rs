use super::write_str;
use crate::formatter::Formatter;
use crate::types::{EncodingType, RdbResult};
use std::io;
use std::io::Write;

pub struct JSON {
    out: Box<dyn Write + 'static>,
    is_first_db: bool,
    has_databases: bool,
    is_first_key_in_db: bool,
    elements_in_key: u32,
    element_index: u32,
}

impl JSON {
    pub fn new() -> JSON {
        let out = Box::new(io::stdout());
        JSON {
            out,
            is_first_db: true,
            has_databases: false,
            is_first_key_in_db: true,
            elements_in_key: 0,
            element_index: 0,
        }
    }
}

fn encode_to_ascii(value: &[u8]) -> String {
    let s = String::from_utf8_lossy(value);
    serde_json::to_string(&s).unwrap()
}

impl JSON {
    fn start_key(&mut self, length: u32) -> RdbResult<()> {
        if !self.is_first_key_in_db {
            write_str(&mut self.out, ",")?;
        }

        self.is_first_key_in_db = false;
        self.elements_in_key = length;
        self.element_index = 0;

        Ok(())
    }

    fn end_key(&mut self) {}

    fn write_comma(&mut self) -> RdbResult<()> {
        if self.element_index > 0 {
            write_str(&mut self.out, ",")?;
        }
        self.element_index += 1;

        Ok(())
    }

    fn write_key(&mut self, key: &[u8]) -> RdbResult<()> {
        self.out.write_all(encode_to_ascii(key).as_bytes())?;

        Ok(())
    }

    fn write_value(&mut self, value: &[u8]) -> RdbResult<()> {
        self.out.write_all(encode_to_ascii(value).as_bytes())?;

        Ok(())
    }
}

impl Formatter for JSON {
    fn start_rdb(&mut self) -> RdbResult<()> {
        write_str(&mut self.out, "[")
    }

    fn end_rdb(&mut self) -> RdbResult<()> {
        if self.has_databases {
            write_str(&mut self.out, "}")?;
        }
        write_str(&mut self.out, "]\n")?;

        Ok(())
    }

    fn start_database(&mut self, _db_number: u32) -> RdbResult<()> {
        if !self.is_first_db {
            write_str(&mut self.out, "},")?;
        }

        write_str(&mut self.out, "{")?;
        self.is_first_db = false;
        self.has_databases = true;
        self.is_first_key_in_db = true;

        Ok(())
    }

    fn set(&mut self, key: &[u8], value: &[u8], _expiry: Option<u64>) -> RdbResult<()> {
        self.start_key(0)?;
        self.write_key(key)?;
        write_str(&mut self.out, ":")?;
        self.write_value(value)?;

        Ok(())
    }

    fn start_hash(
        &mut self,
        key: &[u8],
        length: u32,
        _expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.start_key(length)?;
        self.write_key(key)?;
        write_str(&mut self.out, ":{")?;
        self.out.flush()?;

        Ok(())
    }

    fn end_hash(&mut self, _key: &[u8]) -> RdbResult<()> {
        self.end_key();
        write_str(&mut self.out, "}")?;
        self.out.flush()?;

        Ok(())
    }

    fn hash_element(&mut self, _key: &[u8], field: &[u8], value: &[u8]) -> RdbResult<()> {
        self.write_comma()?;
        self.write_key(field)?;
        write_str(&mut self.out, ":")?;
        self.write_value(value)?;
        self.out.flush()?;

        Ok(())
    }

    fn start_set(
        &mut self,
        key: &[u8],
        cardinality: u32,
        _expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.start_key(cardinality)?;
        self.write_key(key)?;
        write_str(&mut self.out, ":[")?;
        self.out.flush()?;

        Ok(())
    }

    fn end_set(&mut self, _key: &[u8]) -> RdbResult<()> {
        self.end_key();
        write_str(&mut self.out, "]")?;

        Ok(())
    }

    fn set_element(&mut self, _key: &[u8], member: &[u8]) -> RdbResult<()> {
        self.write_comma()?;
        self.write_value(member)?;

        Ok(())
    }

    fn start_list(
        &mut self,
        key: &[u8],
        length: u32,
        _expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.start_key(length)?;
        self.write_key(key)?;
        write_str(&mut self.out, ":[")?;

        Ok(())
    }

    fn end_list(&mut self, _key: &[u8]) -> RdbResult<()> {
        self.end_key();
        write_str(&mut self.out, "]")?;

        Ok(())
    }

    fn list_element(&mut self, _key: &[u8], value: &[u8]) -> RdbResult<()> {
        self.write_comma()?;
        self.write_value(value)?;

        Ok(())
    }

    fn start_sorted_set(
        &mut self,
        key: &[u8],
        length: u32,
        _expiry: Option<u64>,
        _info: EncodingType,
    ) -> RdbResult<()> {
        self.start_key(length)?;
        self.write_key(key)?;
        write_str(&mut self.out, ":{")?;

        Ok(())
    }

    fn end_sorted_set(&mut self, _key: &[u8]) -> RdbResult<()> {
        self.end_key();
        write_str(&mut self.out, "}")?;

        Ok(())
    }

    fn sorted_set_element(&mut self, _key: &[u8], score: f64, member: &[u8]) -> RdbResult<()> {
        self.write_comma()?;
        self.write_key(member)?;
        write_str(&mut self.out, ":")?;
        self.write_value(score.to_string().as_bytes())?;

        Ok(())
    }
}
