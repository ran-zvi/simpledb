mod db;
mod error;
mod test_utils;
mod log;
mod bytes;

use std::path::PathBuf;
// use crate::db::SimpleDB;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, ByteOrder, WriteBytesExt};
use std::io::{Read, Cursor, Write, SeekFrom, Seek};

fn main() {

    std::fs::File::create("Bongo").unwrap();


}
