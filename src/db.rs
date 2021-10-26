use std::fs::remove_file;
use crate::error::{DatabaseError, LockKind};
use crate::log::{Log, LogOperation};

use crate::bytes;

use std::collections::HashMap;
use std::fs::{create_dir, File};
use std::io::{Read, Seek, Write};
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
    path: PathBuf,
    version: u64,
    commit_in_progress: bool,
}

unsafe impl Send for SimpleDB {}
unsafe impl Sync for SimpleDB {}

impl SimpleDB {
    pub fn open(path: PathBuf) -> Result<Self, DatabaseError> {
        if path.exists() {
            SimpleDB::try_load_from_existing(&path)
        } else {
            let records = Arc::new(RwLock::new(HashMap::new()));
            let version = 0;
            create_dir(&path)?;
            create_version_file(&path, version, false)?;

            create_db_file(&path, version, CHECKPOINT_FILE_NAME)?;
            create_db_file(&path, version, LOG_FILE_NAME)?;

            let log_path = get_db_file_path(&path, Some(version), LOG_FILE_NAME);
            let log = Log::<File>::open(&log_path)?;

            Ok(SimpleDB {
                records,
                log,
                path,
                version,
                commit_in_progress: false,
            })
        }
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
        self.commit_in_progress = true;
        let new_version = self.version + 1;

        create_version_file(&self.path, new_version, true)?;

        let mut checkpoint = create_db_file(&self.path, new_version, CHECKPOINT_FILE_NAME)?;
        self.write_records_to_file(&mut checkpoint)?;

        create_db_file(&self.path, new_version, LOG_FILE_NAME)?;
        let log_path = get_db_file_path(&self.path, Some(new_version), LOG_FILE_NAME);
        let log = Log::<File>::open(&log_path)?;

        self.log = log;

        self.commit_in_progress = false;

        self.cleanup_previous_commit_files()
            .expect("Failed to cleanup previous commit files");

        self.version = new_version;

        Ok(())
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    fn try_load_from_existing(path: &Path) -> Result<SimpleDB, DatabaseError> {
        let new_version_file_path = get_db_file_path(path, None, NEW_VERSION_FILE_NAME);
        let version;
        if new_version_file_path.exists() {
            version = read_string_from_file(&new_version_file_path)?.parse::<u64>().unwrap();
            remove_file(new_version_file_path)?;
        }
        else {
            let version_file_path = get_db_file_path(path, None, VERSION_FILE_NAME);
            version = read_string_from_file(&version_file_path)?.parse::<u64>().unwrap();
        }

        let mut checkpoint_file = File::open(&get_db_file_path(path, Some(version), CHECKPOINT_FILE_NAME))?;
        let mut checkpoint: SimpleCollection = match SimpleDB::read_records_from_file(&mut checkpoint_file) {
            Ok(records) => records,
            Err(_) => return Err(DatabaseError::LoadCheckpoint)
        };

        let mut log = Log::<File>::open(&get_db_file_path(path, Some(version), LOG_FILE_NAME))?;
        
        for operation in log.read_until_empty()?.into_iter() {
            match operation {
                LogOperation::Put(key, value) => checkpoint.insert(key.into(), value.into()),
                LogOperation::Delete(key) => checkpoint.remove::<Vec<u8>>(&key.into())
            };
        }

        Ok(SimpleDB {
            records: Arc::new(RwLock::new(checkpoint)),
            path: PathBuf::from(path),
            version,
            log,
            commit_in_progress: false
        })
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

    fn read_records_from_file(file: &mut File) -> Result<SimpleCollection, DatabaseError> {
        let mut records: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        let file_length = file.metadata()?.len();
        while file.stream_position()? < file_length {
            let key_length = bytes::read_u64_from_log(file);
            let key = bytes::read_bytes_from_log(file, key_length)?;

            let value_length = bytes::read_u64_from_log(file);
            let value = bytes::read_bytes_from_log(file, value_length)?;

            records.insert(key, value);
        }
        Ok(records)
    }

    fn get_write_records(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<SimpleCollection>, DatabaseError> {
        if self.commit_in_progress {
            return Err(DatabaseError::Lock {
                kind: LockKind::Write,
                reason: Some(String::from("Commit in progress")),
            });
        }

        match self.records.write() {
            Ok(records) => Ok(records),
            Err(_) => {
                return Err(DatabaseError::Lock {
                    kind: LockKind::Write,
                    reason: None,
                })
            }
        }
    }

    fn cleanup_previous_commit_files(&self) -> std::io::Result<()> {
        std::fs::remove_file(get_db_file_path(
            &self.path,
            Some(self.version),
            LOG_FILE_NAME,
        ))?;
        std::fs::remove_file(get_db_file_path(
            &self.path,
            Some(self.version),
            CHECKPOINT_FILE_NAME,
        ))?;
        std::fs::remove_file(get_db_file_path(&self.path, None, VERSION_FILE_NAME))?;

        let old_version_file_path = get_db_file_path(&self.path, None, VERSION_FILE_NAME);
        let new_version_file_path = get_db_file_path(&self.path, None, NEW_VERSION_FILE_NAME);
        std::fs::rename(new_version_file_path, old_version_file_path)?;

        Ok(())
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
    let file_path = get_db_file_path(&path, Some(version), file_name);
    File::create(file_path)
}

fn get_db_file_path(path: &Path, version: Option<u64>, file_name: &str) -> PathBuf {
    match version {
        Some(n) => PathBuf::from(format!("{}/{}.{}", path.to_str().unwrap(), file_name, n)),
        None => PathBuf::from(format!("{}/{}", path.to_str().unwrap(), file_name,)),
    }
}

fn read_string_from_file(path: &Path) -> std::io::Result<String> {
    let mut file = File::open(path)?;
    let mut string = String::new();
    file.read_to_string(&mut string)?;

    Ok(string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::run_test;
    use serial_test::serial;
    use std::env;
    use std::fs::remove_dir_all;
    use std::io::Read;
    use std::sync::Mutex;

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

    fn _check_file_exists_in_path(base_path: PathBuf, file_name: &str) {
        let mut path = base_path.clone();
        path.push(file_name);
        assert!(path.exists());
    }

    fn check_file_exists_in_temp_test_folder(file_name: &str) {
        let temp_test_path = get_temp_test_current_path();
        _check_file_exists_in_path(temp_test_path, file_name);
    }

    fn get_version_from_file() -> u64 {
        let mut current_path = get_temp_test_current_path();
        current_path.push("version");
        let mut version_file = File::open(current_path).unwrap();

        let mut version = String::new();
        version_file.read_to_string(&mut version).unwrap();

        version.parse::<u64>().unwrap()
    }

    #[test]
    #[serial]
    fn test_create_db() {
        run_test(
            || {
                SimpleDB::open(get_temp_test_current_path()).unwrap();

                check_file_exists_in_temp_test_folder("checkpoint.0");
                check_file_exists_in_temp_test_folder("logfile.0");
                check_file_exists_in_temp_test_folder("version");

                assert_eq!(get_version_from_file(), 0);
            },
            None,
            Some(Box::new(delete_db_files)),
        )
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

    #[test]
    #[serial]
    fn test_concurrent_write() {
        run_test(
            || {
                let db = Arc::new(Mutex::new(
                    SimpleDB::open(get_temp_test_current_path()).unwrap(),
                ));
                let mut handles = vec![];
                for _ in 0..2 {
                    let t_db = Arc::clone(&db);
                    let handle = std::thread::spawn(move || {
                        let mut db = t_db.lock().unwrap();
                        if let None = db.get("name") {
                            db.put("name", "bob").unwrap();
                        } else {
                            db.put("age", "54").unwrap();
                        }
                    });

                    handles.push(handle);
                }
                for handle in handles {
                    handle.join().unwrap();
                }
                let db: &SimpleDB = &*db.lock().unwrap();
                let name = db.get("name").unwrap();
                let age = db.get("age").unwrap();

                assert_eq!(String::from_utf8(name.to_vec()).unwrap(), "bob");
                assert_eq!(String::from_utf8(age.to_vec()).unwrap(), "54");
            },
            None,
            Some(Box::new(delete_db_files)),
        )
    }

    #[test]
    #[serial]
    fn test_commit_changes() {
        run_test(
            || {
                let mut db = SimpleDB::open(get_temp_test_current_path()).unwrap();
                db.put("name", "bob").unwrap();
                db.put("age", "54").unwrap();

                db.commit().unwrap();

                let name = db.get("name").unwrap();
                let age = db.get("age").unwrap();

                assert_eq!(String::from_utf8(name.to_vec()).unwrap(), "bob");
                assert_eq!(String::from_utf8(age.to_vec()).unwrap(), "54");

                check_file_exists_in_temp_test_folder("checkpoint.1");
                check_file_exists_in_temp_test_folder("logfile.1");
                check_file_exists_in_temp_test_folder("version");

                assert_eq!(get_version_from_file(), 1);
                assert_eq!(db.version(), 1);
            },
            None,
            Some(Box::new(delete_db_files)),
        )
    }

    #[test]
    #[serial]
    fn test_load_from_checkpoint_after_commit() {
        run_test(
            || {
                let mut db = SimpleDB::open(get_temp_test_current_path()).unwrap();
                db.put("name", "bob").unwrap();
                db.put("age", "54").unwrap();
                db.delete("age").unwrap();

                db.commit().unwrap();
                
                drop(db);

                let db = SimpleDB::open(get_temp_test_current_path()).unwrap();

                assert_eq!(String::from_utf8(db.get("name").unwrap().to_vec()).unwrap(), "bob");
                assert_eq!(db.version(), 1);
            },
            None,
            Some(Box::new(delete_db_files)),
        )
    }

    #[test]
    #[serial]
    fn test_load_from_checkpoint_before_commit() {
        run_test(
            || {
                let mut db = SimpleDB::open(get_temp_test_current_path()).unwrap();
                db.put("name", "bob").unwrap();
                db.put("age", "54").unwrap();
                db.delete("age").unwrap();
                
                drop(db);

                let db = SimpleDB::open(get_temp_test_current_path()).unwrap();

                assert_eq!(String::from_utf8(db.get("name").unwrap().to_vec()).unwrap(), "bob");
                assert_eq!(db.version(), 0);
            },
            None,
            Some(Box::new(delete_db_files)),
        )
    }
}
