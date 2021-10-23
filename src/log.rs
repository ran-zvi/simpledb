use crate::error::LogError;
use anyhow::Error;
use std::fmt::Debug;
use std::fs::File;
use std::io::SeekFrom;
use std::io::{Read, Seek, Write};
use std::path::Path;
use crate::bytes::{
    U64_BYTES_LEN,
    ReadBytes,
};

use crate::bytes;

#[derive(Debug, PartialEq, Clone)]
pub enum LogOperation {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}


pub struct Log<T: Read + Write + Seek> {
    log: T,
}

impl Log<File> {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let log = File::open(path)?;
        Ok(Log::<File> { log })
    }

    pub fn append_to_disk(&mut self, op: LogOperation) -> Result<(), LogError> {
        self.append(op)?;
        self.log.sync_data()?;
        Ok(())
    }
}

impl<T: Read + Write + Seek> Log<T> {

    pub fn append(&mut self, op: LogOperation) -> Result<(), LogError> {
        let mut bytes: Vec<u8> = vec![];
        match op {
            LogOperation::Put(key, value) => {
                bytes::write_encoded_char_to_buffer('p', &mut bytes);
                bytes::write_encoded_bytes_to_buffer(key, &mut bytes);
                bytes::write_encoded_bytes_to_buffer(value, &mut bytes);
            }
            LogOperation::Delete(key) => {
                bytes::write_encoded_char_to_buffer('d', &mut bytes);
                bytes::write_encoded_bytes_to_buffer(key, &mut bytes);
            }
        }
        self.log.seek(SeekFrom::End(0))?;
        self.log.write_all(&bytes)?;
        Ok(())
    }


    pub fn read_until_empty(&mut self) -> Result<Vec<LogOperation>, LogError> {
        let mut log_operations = vec![];

        self.log.rewind()?;
        let mut end_reached = false;
        while !end_reached {
            if let Ok(op) = self.read_operation_from_log() {
                log_operations.push(op);
            }
            else {
                end_reached = true;
            }
        }

        Ok(log_operations)
    }


    fn read_operation_from_log(&mut self) -> Result<LogOperation, LogError> {
        let mut op_len_buf = [0; 9];
        
        match self.log.read_exact(&mut op_len_buf) {
            Ok(()) => (),
            Err(e) => return Err(LogError::EndReached.into())
        }

        let op = op_len_buf[U64_BYTES_LEN] as char;
        match op {
            'p' => {
                let key = self.read_instruction_from_log();
                let value = self.read_instruction_from_log();
                
                Ok(LogOperation::Put(key, value))
            }
            'd' => {
                let key = self.read_instruction_from_log();
            
                Ok(LogOperation::Delete(key))
            }
            c => Err(LogError::InvalidOperation(c)),
        }
    }

    fn read_instruction_from_log(&mut self) -> Vec<u8> {
        let instruction_length = self.read_u64_from_log();
        match self.read_bytes_from_log(instruction_length as usize) {
            Ok(bytes) => bytes,
            Err(e) => panic!("Unable to read instruction from log: {}", e)
        }
    }


}

impl<T: Read + Write + Seek> ReadBytes for Log<T> {
    fn read_bytes_from_log(&mut self, bytes_length: usize) -> std::io::Result<Vec<u8>> {
        let mut buf: Vec<u8> = vec![0u8; bytes_length as usize];
        self.log.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn read_u64_from_log(&mut self) -> u64 {
        let mut len_buf = [0; U64_BYTES_LEN];
        self.log.read_exact(&mut len_buf).expect("Failed reading u64 from log");

        u64::from_be_bytes(len_buf)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_log() {
        let cursor = Cursor::new(Vec::new());
        let mut log = Log { log: cursor} ;

        let expected_op_1 = LogOperation::Put("Hello".into(), "World".into());
        let expected_op_2 = LogOperation::Delete("Hello".into());

        log.append(expected_op_1.clone()).unwrap();
        log.append(expected_op_2.clone()).unwrap();

        let ops = log.read_until_empty().unwrap();

        assert_eq!(vec![expected_op_1, expected_op_2], ops);
    }

}
