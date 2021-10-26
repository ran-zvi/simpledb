use std::io::Read;
use std::io::Seek;
use std::io::Write;

pub const U64_BYTES_LEN: usize = 8;

pub fn write_encoded_char_to_buffer(c: char, buf: &mut Vec<u8>) -> () {
    buf.extend((1 as u64).to_be_bytes());
    buf.push(c as u8);
}

pub fn write_encoded_bytes_to_buffer(bytes: Vec<u8>, buf: &mut Vec<u8>) -> () {
    buf.extend(encode_be_u64(bytes.len()));
    buf.extend(bytes);
}

pub fn read_bytes_from_log<T: Read + Seek>(
    reader: &mut T,
    bytes_length: u64,
) -> std::io::Result<Vec<u8>> {
    let mut buf: Vec<u8> = vec![0u8; bytes_length as usize];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn read_u64_from_log<T: Read + Seek>(reader: &mut T) -> u64 {
    let mut len_buf = [0; U64_BYTES_LEN];
    reader
        .read_exact(&mut len_buf)
        .expect("Failed reading u64 from log");

    u64::from_be_bytes(len_buf)
}

fn encode_be_u64(n: usize) -> [u8; U64_BYTES_LEN] {
    (n as u64).to_be_bytes()
}
