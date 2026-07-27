use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum OccError {
    #[error("Transaction failed validation due to conflict and was aborted")]
    ValidationFailed,
    #[error("User manually aborted transaction: {0}")]
    UserAbort(String),
}
