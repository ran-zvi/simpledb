use thiserror::Error;

#[derive(Debug)]
pub enum LockKind {
    Read,
    Write
}

#[derive(Error, Debug)]
pub enum DatabaseError {
    
    #[error("Database creation failed")]
    Initialization,

    #[error("Unable to acquire lock: {kind:?}, reason: {reason:?}")]
    Lock {
        kind: LockKind,
        reason: Option<String>
    },
    

    #[error("Database files not found on disk")]
    NotFound(#[from] std::io::Error),

    #[error("Key: {0} doesn't exist in the Databse")]
    KeyNotFound(String),


    #[error("Failed to load records from checkpoint")]
    LoadCheckpoint,

    #[error(transparent)]
    Other(#[from] anyhow::Error)
}

#[derive(Error, Debug)]
pub enum LogError {

    #[error("End of log reached")]
    EndReached,

    #[error("Invalid log operation: {0}")]
    InvalidOperation(char),

    #[error("Failed to perform IO operations on the log")]
    Io(#[from] std::io::Error)
}

impl From<LogError> for DatabaseError {
    fn from(error: LogError) -> Self {
        DatabaseError::Other(anyhow::Error::new(error))
    }}