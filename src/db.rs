use crate::bytes::{ReadBytes, U64_BYTES_LEN};
use crate::error::{DatabaseError, LockKind};
use crate::log::{Log, LogOperation};

use crate::bytes;

use std::collections::HashMap;
use std::fs::{create_dir, File};
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

type SimpleCollection = HashMap<Vec<u8>, Vec<u8>>;
type Records = Arc<RwLock<SimpleCollection>>;

const CHECKPOINT_FILE_NAME: &str = "checkpoint";
const LOG_FILE_NAME: &str = "logfile";
const VERSION_FILE_NAME: &str = "version";
const NEW_VERSION_FILE_NAME: &str = "new_version";

pub struct SimpleDB {
    records: Records,
    log: Log<File>,
    checkpoint: File,
    path: PathBuf,
    version: u64,
    active_commit: bool,
}

impl SimpleDB {
    pub fn open(path: PathBuf) -> Result<Self, DatabaseError> {
        // if path.exists() {
        //     SimpleDB::try_load_from_existing(&path)
        // } else {
        let records = Arc::new(RwLock::new(HashMap::new()));
        let version = 0;
        create_dir(&path)?;
        create_version_file(&path, version, false)?;

        let checkpoint = create_db_file(&path, version, CHECKPOINT_FILE_NAME)?;
        let log_path = get_db_file_path(&path, Some(version), LOG_FILE_NAME);
        let log = Log::<File>::open(&log_path)?;

        Ok(SimpleDB {
            records,
            log,
            checkpoint,
            path,
            version,
            active_commit: false,
        })
        // }
    }

    pub fn get<S: Into<Vec<u8>>>(&self, key: S) -> Option<Vec<u8>> {
        self.records
            .read()
            .ok()
            .and_then(|records| records.get(&key.into()).map(|val| val.clone()))
    }

    pub fn put<S: Into<Vec<u8>>, V: Into<Vec<u8>>>(
        &mut self,
        key: S,
        value: V,
    ) -> Result<(), DatabaseError> {
        let key_as_bytes: Vec<u8> = key.into();
        let value_as_bytes: Vec<u8> = value.into();

        self.log.append_to_disk(LogOperation::Put(
            key_as_bytes.clone(),
            value_as_bytes.clone(),
        ))?;

        let mut records = self.get_write_records()?;
        (*records).insert(key_as_bytes, value_as_bytes);
        Ok(())
    }

    pub fn delete<S: Into<Vec<u8>>>(&mut self, key: S) -> Result<(), DatabaseError> {
        let key_as_bytes: Vec<u8> = key.into();
        self.log
            .append_to_disk(LogOperation::Delete(key_as_bytes.clone()))?;
        let mut records = self.get_write_records()?;
        (*records).remove(&key_as_bytes);

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), DatabaseError> {
        self.active_commit = true;

        let new_version = self.version + 1;

        create_version_file(&self.path, new_version, true)?;

        let mut checkpoint = create_db_file(&self.path, new_version, CHECKPOINT_FILE_NAME)?;
        self.write_records_to_file(&mut checkpoint)?;

        let log_path = get_db_file_path(&self.path, Some(new_version), LOG_FILE_NAME);
        let log = Log::<File>::open(&log_path)?;

        self.checkpoint = checkpoint;
        self.log = log;

        let old_version_file_path = get_db_file_path(&self.path, None, VERSION_FILE_NAME);
        let new_version_file_path = get_db_file_path(&self.path, None, NEW_VERSION_FILE_NAME);        

        std::fs::remove_file(get_db_file_path(&self.path, Some(self.version), LOG_FILE_NAME))?;
        std::fs::remove_file(get_db_file_path(&self.path, Some(self.version), CHECKPOINT_FILE_NAME))?;
        std::fs::remove_file(get_db_file_path(&self.path, None, VERSION_FILE_NAME))?;
        std::fs::rename(old_version_file_path, new_version_file_path);

        self.version = new_version;


        self.active_commit = false;
        Ok(())
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    fn write_records_to_file(&self, file: &mut File) -> Result<(), DatabaseError> {
        let mut buffer = vec![];
        let records = match self.records.read() {
            Ok(ro_records) => ro_records,
            Err(_) => {
                return Err(DatabaseError::Lock {
                    kind: LockKind::Read,
                    reason: None,
                })
            }
        };

        for (key, value) in records.iter() {
            bytes::write_encoded_bytes_to_buffer(key.to_vec(), &mut buffer);
            bytes::write_encoded_bytes_to_buffer(value.to_vec(), &mut buffer);
        }

        file.write_all(&buffer)?;
        file.sync_data()?;

        Ok(())
    }

    fn get_write_records(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<SimpleCollection>, DatabaseError> {
        if self.active_commit {
            return Err(DatabaseError::Lock {
                kind: LockKind::Write,
                reason: Some(String::from("Commit in progress")),
            });
        }

        match self.records.write() {
            Ok(records) => Ok(records),
            Err(e) => {
                return Err(DatabaseError::Lock {
                    kind: LockKind::Write,
                    reason: None,
                })
            }
        }
    }
}

fn create_version_file(path: &Path, version: u64, new: bool) -> std::io::Result<()> {
    let file_name = if new {
        NEW_VERSION_FILE_NAME
    } else {
        VERSION_FILE_NAME
    };
    let file_path = format!("{}/{}", path.to_str().unwrap(), file_name);
    let mut file = File::create(file_path)?;
    let version_string = format!("{}", version);
    file.write_all(version_string.as_bytes())?;

    Ok(())
}

fn create_db_file(path: &Path, version: u64, file_name: &str) -> std::io::Result<File> {
    let file_path = format!("{}/{}.{}", path.to_str().unwrap(), file_name, version);
    File::create(file_path)
}

fn get_db_file_path(path: &Path, version: Option<u64>, file_name: &str) -> PathBuf {
    match version {
        Some(n) => PathBuf::from(format!(
            "{}/{}.{}",
            path.to_str().unwrap(),
            file_name,
            n
        )),
        None => PathBuf::from(format!(
            "{}/{}",
            path.to_str().unwrap(),
            file_name,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::run_test;
    use serial_test::serial;
    use std::env;
    use std::fs::remove_dir_all;
    use std::io::Read;

    fn get_temp_test_current_path() -> PathBuf {
        env::current_dir()
            .and_then(|mut p| {
                p.push("_temp_test");
                Ok(p)
            })
            .unwrap()
    }

    fn delete_db_files() {
        let temp_test_path = get_temp_test_current_path();
        remove_dir_all(temp_test_path).unwrap();
    }

    fn check_file_exists_in_path(base_path: PathBuf, file_name: &str) {
        let mut path = base_path.clone();
        path.push(file_name);
        assert!(path.exists());
    }

    fn check_file_exists_in_temp_test_folder(file_name: &str) {
        let temp_test_path = get_temp_test_current_path();
        check_file_exists_in_path(temp_test_path, file_name);
    }

    #[test]
    #[serial]
    fn test_basic_db_operations() {
        run_test(
            || {
                let mut db = SimpleDB::open(get_temp_test_current_path()).unwrap();
                db.put("name", "ran").unwrap();
                let name = db.get("name").unwrap();

                assert_eq!(String::from_utf8(name.to_vec()).unwrap(), "ran");

                db.put("name", "bob").unwrap();
                let name = db.get("name").unwrap();
                assert_eq!(String::from_utf8(name.to_vec()).unwrap(), "bob");
                assert_eq!(db.version(), 0);
            },
            None,
            Some(Box::new(delete_db_files)),
        )
    }
}
