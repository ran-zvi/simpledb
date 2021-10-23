use std::io::Read;

pub const U64_BYTES_LEN: usize = 8;

pub fn write_encoded_char_to_buffer(c: char, buf: &mut Vec<u8>) -> () {
    buf.extend((1 as u64).to_be_bytes());
    buf.push(c as u8);
}

pub fn write_encoded_bytes_to_buffer(bytes: Vec<u8>, buf: &mut Vec<u8>) -> () {
    buf.extend(encode_be_u64(bytes.len()));
    buf.extend(bytes);
}

fn encode_be_u64(n: usize) -> [u8; U64_BYTES_LEN] {
    (n as u64).to_be_bytes()
}

pub trait ReadBytes {
    fn read_bytes_from_log(&mut self, bytes_length: usize) -> std::io::Result<Vec<u8>>;

    fn read_u64_from_log(&mut self) -> u64;
}