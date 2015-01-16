//! rdb - Parse, analyze and dump RDB files
//!
//! A RDB file is a binary representation of the in-memory data of Redis.
//! This binary file is sufficient to completely restore Redis’ state.
//!
//! This library provides the methods to parse and analyze a RDB file
//! and to reformat and dump it in another format such as JSON or
//! RESP, the Redis Serialization.
//!
//! You can depend on this library via Cargo:
//!
//! ```ini
//! [dependencies]
//! rdb = "0.5.0"
//! ```
//!
//! # Basic operation
//!
//! rdb-rs exposes just one important method: `parse`.
//! This methods takes care of reading the RDB from a stream,
//! parsing the containted data and calling the provided formatter with already-parsed values.
//!
//! ```rust,no_run
//! # #![allow(unstable)]
//! # use std::io::{BufferedReader, File};
//! let file = File::open(&Path::new("dump.rdb"));
//! let reader = BufferedReader::new(file);
//! rdb::parse(reader, rdb::JSONFormatter::new())
//! ```
//!
//! # Formatter
//!
//! rdb-rs brings 4 pre-defined formatters, which can be used:
//!
//! * `PlainFormatter`: Just plain output for testing
//! * `JSONFormatter`: JSON-encoded output
//! * `NilFormatter`: Surpresses all output
//! * `ProtocolFormatter`: Formats the data in [RESP](http://redis.io/topics/protocol),
//! the Redis Serialization Protocol
//!
//! These formatters adhere to the `RdbParseFormatter` trait
//! and supply a method for each possible datatype or opcode.
//! Its up to the formatter to correctly handle all provided data such as
//! lists, sets, hashes, expires and metadata.
//!
//! # Command-line
//!
//! rdb-rs brings a Command Line application as well.
//!
//! This application will take a RDB file as input and format it in the specified format (JSON by
//! default).
//!
//! Example:
//!
//! ```shell,no_compile
//! $ rdb --format json dump.rdb
//! [{"key":"value"}]
//! $ rdb --format protocol dump.rdb
//! *2
//! $6
//! SELECT
//! $1
//! 0
//! *3
//! $3
//! SET
//! $3
//! key
//! $5
//! value
//! ```

#![feature(slicing_syntax)]
#![allow(unstable)]

extern crate lzf;
extern crate serialize;
extern crate regex;

use std::str;
use lzf::decompress;
use std::io::MemReader;
use regex::Regex;

pub use formatter::RdbParseFormatter;
pub use nil_formatter::NilFormatter;
pub use plain_formatter::PlainFormatter;
pub use json_formatter::JSONFormatter;
pub use protocol_formatter::ProtocolFormatter;

mod helper;

pub mod formatter;
pub mod nil_formatter;
pub mod plain_formatter;
pub mod json_formatter;
pub mod protocol_formatter;

mod version {
    pub const SUPPORTED_MINIMUM : u32 = 1;
    pub const SUPPORTED_MAXIMUM : u32 = 7;
}

mod constants {
    pub const RDB_6BITLEN : u8 = 0;
    pub const RDB_14BITLEN : u8 = 1;
    pub const RDB_ENCVAL : u8 = 3;
    pub const RDB_MAGIC : &'static str = "REDIS";
}

mod op_codes {
    pub const AUX : u8 = 250;
    pub const RESIZEDB : u8 = 251;
    pub const EXPIRETIME_MS : u8 = 252;
    pub const EXPIRETIME : u8 = 253;
    pub const SELECTDB   : u8 = 254;
    pub const EOF : u8 = 255;
}

mod types {
    pub const STRING : u8 = 0;
    pub const LIST : u8 = 1;
    pub const SET : u8 = 2;
    pub const ZSET : u8 = 3;
    pub const HASH : u8 = 4;
    pub const HASH_ZIPMAP : u8 = 9;
    pub const LIST_ZIPLIST : u8 = 10;
    pub const SET_INTSET : u8 = 11;
    pub const ZSET_ZIPLIST : u8 = 12;
    pub const HASH_ZIPLIST : u8 = 13;
    pub const LIST_QUICKLIST : u8 = 14;
}

mod encoding {
    pub const INT8 : u32 = 0;
    pub const INT16 : u32 = 1;
    pub const INT32 : u32 = 2;
    pub const LZF : u32 = 3;
}

#[derive(Copy,PartialEq)]
pub enum Type {
    String,
    List,
    Set,
    SortedSet,
    Hash
}

impl Type {
    fn from_encoding(enc_type: u8) -> Type {
        match enc_type {
            types::STRING => Type::String,
            types::HASH | types::HASH_ZIPMAP | types::HASH_ZIPLIST => Type::Hash,
            types::LIST | types::LIST_ZIPLIST => Type::List,
            types::SET | types::SET_INTSET => Type::Set,
            types::ZSET | types::ZSET_ZIPLIST => Type::SortedSet,
            _ => { panic!("Unknown encoding type: {}", enc_type) }
        }
    }
}

#[derive(Show,Clone)]
pub enum DataType {
    String(Vec<u8>),
    Number(i64),
    ListOfTypes(Vec<DataType>),
    HashOfTypes(Vec<DataType>),
    SortedSetOfTypes(Vec<DataType>),
    Intset(Vec<i64>),
    List(Vec<Vec<u8>>),
    Set(Vec<Vec<u8>>),
    SortedSet(Vec<(f64,Vec<u8>)>),
    Hash(Vec<Vec<u8>>),
    Unknown
}

pub trait RdbFilter {
    fn matches_db(&self, _db: u32) -> bool { true }
    fn matches_type(&self, _enc_type: u8) -> bool { true }
    fn matches_key(&self, _key: &[u8]) -> bool { true }

}

#[derive(Copy)]
pub struct AllFilter;
impl RdbFilter for AllFilter {}

pub struct StrictFilter {
    databases: Vec<u32>,
    types: Vec<Type>,
    keys: Option<Regex>
}

impl StrictFilter {
    pub fn new() -> StrictFilter {
        StrictFilter { databases: vec![], types: vec![], keys: None }
    }

    pub fn add_database(&mut self, db: u32) {
        self.databases.push(db);
    }

    pub fn add_type(&mut self, typ: Type) {
        self.types.push(typ);
    }

    pub fn add_keys(&mut self, re: Regex) {
        self.keys = Some(re);
    }
}

impl RdbFilter for StrictFilter {
    fn matches_db(&self, db: u32) -> bool {
        if self.databases.is_empty() {
            true
        } else {
            self.databases.iter().any(|&x| x == db)
        }
    }

    fn matches_type(&self, enc_type: u8) -> bool {
        if self.types.is_empty() {
            return true
        }

        let typ = Type::from_encoding(enc_type);
        self.types.iter().any(|&x| x == typ)
    }

    fn matches_key(&self, key: &[u8]) -> bool {
        match self.keys.clone() {
            None => true,
            Some(re) => {
                let key = unsafe{str::from_utf8_unchecked(key)};
                re.is_match(key)
            }
        }
    }
}

pub struct RdbParser<R: Reader, F: RdbParseFormatter, L: RdbFilter> {
    input: R,
    formatter: F,
    filter: L,
    last_expiretime: Option<u64>
}

pub fn read_length_with_encoding<R: Reader>(input: &mut R) -> (u32, bool) {
    let mut length;
    let mut is_encoded = false;

    let enc_type = input.read_byte().unwrap();

    match (enc_type & 0xC0) >> 6 {
        constants::RDB_ENCVAL => {
            is_encoded = true;
            length = (enc_type & 0x3F) as u32;
        },
        constants::RDB_6BITLEN => {
            length = (enc_type & 0x3F) as u32;
        },
        constants::RDB_14BITLEN => {
            let next_byte = input.read_byte().unwrap();
            length = (((enc_type & 0x3F) as u32) <<8) | next_byte as u32;
        },
        _ => {
            length = input.read_be_u32().unwrap();
        }
    }

    (length, is_encoded)
}

pub fn read_length<R: Reader>(input: &mut R) -> u32 {
    let (length, _) = read_length_with_encoding(input);
    length
}

pub fn verify_magic<R: Reader>(input: &mut R) -> bool {
    let magic = input.read_exact(5).unwrap();

    // Meeeeeh.
    magic[0] == constants::RDB_MAGIC.as_bytes()[0] &&
        magic[1] == constants::RDB_MAGIC.as_bytes()[1] &&
        magic[2] == constants::RDB_MAGIC.as_bytes()[2] &&
        magic[3] == constants::RDB_MAGIC.as_bytes()[3] &&
        magic[4] == constants::RDB_MAGIC.as_bytes()[4]
}

pub fn verify_version<R: Reader>(input: &mut R) -> bool {
    let version = input.read_exact(4).unwrap();

    let version = (version[0]-48) as u32 * 1000 +
        (version[1]-48) as u32 * 100 +
        (version[2]-48) as u32 * 10 +
        (version[3]-48) as u32;

    version >= version::SUPPORTED_MINIMUM &&
        version <= version::SUPPORTED_MAXIMUM
}

pub fn read_blob<R: Reader>(input: &mut R) -> Vec<u8> {
    let (length, is_encoded) = read_length_with_encoding(input);

    if is_encoded {
        match length {
            encoding::INT8 => { helper::int_to_vec(input.read_i8().unwrap() as i32) },
            encoding::INT16 => { helper::int_to_vec(input.read_le_i16().unwrap() as i32) },
            encoding::INT32 => { helper::int_to_vec(input.read_le_i32().unwrap() as i32) },
            encoding::LZF => {
                let compressed_length = read_length(input);
                let real_length = read_length(input);
                let data = input.read_exact(compressed_length as usize).unwrap();
                lzf::decompress(data.as_slice(), real_length as usize).unwrap()
            },
            _ => { panic!("Unknown encoding: {}", length) }
        }
    } else {
        input.read_exact(length as usize).unwrap()
    }
}

fn read_ziplist_metadata<T: Reader>(input: &mut T) -> (u32, u32, u16) {
    let zlbytes = input.read_le_u32().unwrap();
    let zltail = input.read_le_u32().unwrap();
    let zllen = input.read_le_u16().unwrap();

    (zlbytes, zltail, zllen)
}

pub fn parse<R: Reader, F: RdbParseFormatter, T: RdbFilter>(input: R, formatter: F, filter: T) {
    let mut parser = RdbParser::new(input, formatter, filter);
    parser.parse()
}

impl<R: Reader, F: RdbParseFormatter, L: RdbFilter> RdbParser<R, F, L> {
    pub fn new(input: R, formatter: F, filter: L) -> RdbParser<R, F, L> {
        RdbParser{
            input: input,
            formatter: formatter,
            filter: filter,
            last_expiretime: None
        }
    }

    pub fn parse(&mut self) {
        assert!(verify_magic(&mut self.input));
        assert!(verify_version(&mut self.input));

        self.formatter.start_rdb();

        let mut last_database : u32 = 0;
        loop {
            let next_op = self.input.read_byte().unwrap();

            match next_op {
                op_codes::SELECTDB => {
                    last_database = read_length(&mut self.input);
                    if self.filter.matches_db(last_database) {
                        self.formatter.start_database(last_database);
                    }
                },
                op_codes::EOF => {
                    self.formatter.end_database(last_database);
                    self.formatter.end_rdb();

                    let checksum = self.input.read_to_end().unwrap();
                    self.formatter.checksum(checksum.as_slice());
                    break;
                },
                op_codes::EXPIRETIME_MS => {
                    let expiretime_ms = self.input.read_le_u64().unwrap();
                    self.last_expiretime = Some(expiretime_ms);
                },
                op_codes::EXPIRETIME => {
                    let expiretime = self.input.read_be_u32().unwrap();
                    self.last_expiretime = Some(expiretime as u64 * 1000);
                },
                op_codes::RESIZEDB => {
                    let db_size = read_length(&mut self.input);
                    let expires_size = read_length(&mut self.input);

                    self.formatter.resizedb(db_size, expires_size);
                },
                op_codes::AUX => {
                    let auxkey = read_blob(&mut self.input);
                    let auxval = read_blob(&mut self.input);

                    self.formatter.aux_field(
                        auxkey.as_slice(),
                        auxval.as_slice());
                },
                _ => {
                    if self.filter.matches_db(last_database) {
                        let key = read_blob(&mut self.input);

                        if self.filter.matches_type(next_op) && self.filter.matches_key(key.as_slice()) {
                            self.read_type(key.as_slice(), next_op);
                        } else {
                            self.skip_object(next_op);
                        }
                    } else {
                        self.skip_key_and_object(next_op);
                    }

                    self.last_expiretime = None;
                }
            }

        }
    }

    fn read_linked_list(&mut self, key: &[u8]) -> Vec<Vec<u8>> {
        let mut len = read_length(&mut self.input);
        let mut list = vec![];

        self.formatter.start_list(key, len, self.last_expiretime, None);

        while len > 0 {
            let blob = read_blob(&mut self.input);
            self.formatter.list_element(key, blob.as_slice());
            list.push(blob);
            len -= 1;
        }

        self.formatter.end_list(key);

        list
    }

    fn read_sorted_set(&mut self, key: &[u8]) -> Vec<(f64,Vec<u8>)> {
        let mut set = vec![];
        let mut set_items = read_length(&mut self.input);

        self.formatter.start_sorted_set(key, set_items, self.last_expiretime, None);

        while set_items > 0 {
            let val = read_blob(&mut self.input);
            let score_length = self.input.read_byte().unwrap();
            let score = match score_length {
                253 => { std::f64::NAN },
                254 => { std::f64::INFINITY },
                255 => { std::f64::NEG_INFINITY },
                _ => {
                    let tmp = self.input.read_exact(score_length as usize).unwrap();
                    unsafe{str::from_utf8_unchecked(tmp.as_slice())}.
                        parse::<f64>().unwrap()
                }
            };

            self.formatter.sorted_set_element(key, score, val.as_slice());
            set.push((score, val));

            set_items -= 1;
        }

        self.formatter.end_sorted_set(key);

        set
    }

    fn read_hash(&mut self, key: &[u8]) -> Vec<Vec<u8>> {
        let mut hash = vec![];
        let mut hash_items = read_length(&mut self.input);

        self.formatter.start_hash(key, hash_items, self.last_expiretime, None);

        while hash_items > 0 {
            let field = read_blob(&mut self.input);
            let val = read_blob(&mut self.input);

            self.formatter.hash_element(key, field.as_slice(), val.as_slice());

            hash.push(field);
            hash.push(val);

            hash_items -= 1;
        }

        self.formatter.end_hash(key);

        hash
    }

    fn read_ziplist_entry<T: Reader>(&mut self, ziplist: &mut T) -> DataType {
        // 1. 1 or 5 bytes length of previous entry
        match ziplist.read_byte().unwrap() {
            254 => {
                let _ = ziplist.read_exact(4).unwrap();
            },
            _ => {}
        }

        let mut length : u64;
        let mut number_value : i64;

        // 2. Read flag or number value
        let flag = ziplist.read_byte().unwrap();

        match (flag & 0xC0) >> 6 {
            0 => { length = (flag & 0x3F) as u64 },
            1 => {
                let next_byte = ziplist.read_byte().unwrap();
                length = (((flag & 0x3F) as u64) << 8) | next_byte as u64;
            },
            2 => {
                length = ziplist.read_be_u32().unwrap() as u64;
            },
            _ => {
                match (flag & 0xF0) >> 4 {
                    0xC => { number_value = ziplist.read_le_i16().unwrap() as i64 },
                    0xD => { number_value = ziplist.read_le_i32().unwrap() as i64 },
                    0xE => { number_value = ziplist.read_le_i64().unwrap() as i64 },
                    0xF => {
                        match flag & 0xF {
                            0 => {
                                let bytes = ziplist.read_exact(3).unwrap();
                                number_value = ((bytes[0] as i64) << 16) ^
                                    ((bytes[1] as i64) << 8) ^
                                    (bytes[2] as i64);
                            },
                            0xE => {
                                number_value = ziplist.read_byte().unwrap() as i64 },
                                _ => { number_value = (flag & 0xF) as i64 - 1; }
                        }
                    },
                    _ => {
                        panic!("Flag not handled: {}", flag);
                    }

                }

                return DataType::Number(number_value)
            }
        }

        // 3. Read value
        let rawval = ziplist.read_exact(length as usize).unwrap();
        DataType::String(rawval)
    }

    fn read_ziplist_entries<T: Reader>(&mut self, reader: &mut T, key: &[u8], zllen: u16) -> Vec<DataType> {
        let mut list = Vec::with_capacity(zllen as usize);

        for _ in (0..zllen) {
            let entry = self.read_ziplist_entry(reader);
            match entry {
                DataType::String(ref val) => {
                    self.formatter.list_element(key, val.as_slice());
                },
                DataType::Number(val) => {
                    self.formatter.list_element(key, val.to_string().as_bytes());
                },
                _ => unreachable!()
            }
            list.push(entry);
        }
        list
    }

    fn read_list_ziplist(&mut self, key: &[u8]) -> Vec<DataType> {
        let ziplist = read_blob(&mut self.input);

        let mut reader = MemReader::new(ziplist);
        let (_zlbytes, _zltail, zllen) = read_ziplist_metadata(&mut reader);

        self.formatter.start_list(key, zllen as u32, self.last_expiretime, None);

        let list = self.read_ziplist_entries(&mut reader, key, zllen);

        assert!(reader.read_byte().unwrap() == 0xFF);
        self.formatter.end_list(key);

        list
    }

    fn read_quicklist_ziplist(&mut self, key: &[u8]) -> Vec<DataType> {
        let ziplist = read_blob(&mut self.input);

        let mut reader = MemReader::new(ziplist);
        let (_zlbytes, _zltail, zllen) = read_ziplist_metadata(&mut reader);

        let list = self.read_ziplist_entries(&mut reader, key, zllen);

        assert!(reader.read_byte().unwrap() == 0xFF);

        list
    }

    fn read_zipmap_entry<T: Reader>(&mut self, next_byte: u8, zipmap: &mut T) -> Vec<u8> {
        let mut elem_len;
        match next_byte {
            253 => { elem_len = zipmap.read_le_u32().unwrap() },
            254 | 255 => {
                panic!("Invalid length value in zipmap: {}", next_byte)
            },
            _ => { elem_len = next_byte as u32 }
        }

        zipmap.read_exact(elem_len as usize).unwrap()
    }

    fn read_hash_zipmap(&mut self) -> Vec<Vec<u8>> {
        let zipmap = read_blob(&mut self.input);

        let mut reader = MemReader::new(zipmap);

        let zmlen = reader.read_byte().unwrap();

        let mut length;
        let mut hash;
        if zmlen <= 254 {
            length = zmlen as usize;
            hash = Vec::with_capacity(length);
        } else {
            length = -1;
            hash = Vec::with_capacity(255);
        }

        loop {
            let next_byte = reader.read_byte().unwrap();

            if next_byte == 0xFF {
                break; // End of list.
            }

            let key = self.read_zipmap_entry(next_byte, &mut reader);
            hash.push(key);

            let next_byte = reader.read_byte().unwrap();
            let _free = reader.read_byte().unwrap();
            let value = self.read_zipmap_entry(next_byte, &mut reader);
            hash.push(value);

            if length > 0 {
                length -= 1;
            }

            if length == 0 {
                assert!(reader.read_byte().unwrap() == 0xFF);
                break;
            }
        }

        hash
    }

    fn read_set_intset(&mut self, key: &[u8]) -> Vec<i64> {
        let mut set = vec![];

        let intset = read_blob(&mut self.input);

        let mut reader = MemReader::new(intset);
        let byte_size = reader.read_le_u32().unwrap();
        let intset_length = reader.read_le_u32().unwrap();

        self.formatter.start_set(key, intset_length, self.last_expiretime, None);

        for _ in (0..intset_length) {
            let val = match byte_size {
                2 => reader.read_le_i16().unwrap() as i64,
                4 => reader.read_le_i32().unwrap() as i64,
                8 => reader.read_le_i64().unwrap(),
                _ => panic!("unhandled byte size in intset: {}", byte_size)
            };

            self.formatter.set_element(key, val.to_string().as_bytes());
            set.push(val);
        }

        self.formatter.end_set(key);
        set
    }

    fn read_quicklist(&mut self, key: &[u8]) -> Vec<DataType> {
        let len = read_length(&mut self.input);

        // FIXME: We don't know the real length here
        // Not sure how we do it correctly
        // Also: We can't call read_list_ziplist as is
        self.formatter.start_set(key, 0, self.last_expiretime, None);
        let mut list = vec![];
        for _ in (0..len) {
            let zl = self.read_quicklist_ziplist(key);
            list.push_all(zl.as_slice());
        }
        self.formatter.end_set(key);
        list
    }

    fn read_type(&mut self, key: &[u8], value_type: u8) -> DataType {
        match value_type {
            types::STRING => {
                let val = read_blob(&mut self.input);
                self.formatter.set(key, val.as_slice(), self.last_expiretime);
                DataType::String(val)
            },
            types::LIST => {
                DataType::List(self.read_linked_list(key))
            },
            types::SET => {
                DataType::Set(self.read_linked_list(key))
            },
            types::ZSET => {
                DataType::SortedSet(self.read_sorted_set(key))
            },
            types::HASH => {
                DataType::Hash(self.read_hash(key))
            },
            types::HASH_ZIPMAP => {
                DataType::Hash(self.read_hash_zipmap())
            },
            types::LIST_ZIPLIST => {
                DataType::ListOfTypes(self.read_list_ziplist(key))
            },
            types::SET_INTSET => {
                DataType::Intset(self.read_set_intset(key))
            },
            types::ZSET_ZIPLIST => {
                DataType::SortedSetOfTypes(self.read_list_ziplist(key))
            },
            types::HASH_ZIPLIST => {
                DataType::ListOfTypes(self.read_list_ziplist(key))
            },
            types::LIST_QUICKLIST => {
                DataType::ListOfTypes(self.read_quicklist(key))
            },
            _ => { panic!("Value Type not implemented: {}", value_type) }
        }
    }

    fn skip(&mut self, skip_bytes: usize) {
        let _ = self.input.read_exact(skip_bytes);
    }

    fn skip_blob(&mut self) {
        let (len, is_encoded) = read_length_with_encoding(&mut self.input);
        let mut skip_bytes;

        if is_encoded {
            skip_bytes = match len {
                encoding::INT8 => 1,
                encoding::INT16 => 2,
                encoding::INT32 => 4,
                encoding::LZF => {
                    let compressed_length = read_length(&mut self.input);
                    let _real_length = read_length(&mut self.input);
                    compressed_length
                },
                _ => { panic!("Unknown encoding: {}", len) }
            }
        } else {
            skip_bytes = len;
        }

        self.skip(skip_bytes as usize);
    }

    fn skip_object(&mut self, enc_type: u8) {
        let blobs_to_skip = match enc_type {
            types::STRING |
                types::HASH_ZIPMAP |
                types::LIST_ZIPLIST |
                types::SET_INTSET |
                types::ZSET_ZIPLIST |
                types::HASH_ZIPLIST => 1,
            types::LIST | types::SET => read_length(&mut self.input),
            types::ZSET | types::HASH => read_length(&mut self.input) * 2,
            _ => { panic!("Unknown encoding type: {}", enc_type) }
        };

        for _ in (0..blobs_to_skip) {
            self.skip_blob()
        }
    }

    fn skip_key_and_object(&mut self, enc_type: u8) {
        self.skip_blob();
        self.skip_object(enc_type);
    }
}
