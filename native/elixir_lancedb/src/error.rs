use lancedb::Error as LanceError;
use rustler::{Encoder, Env, Error as RustlerError, Term};

use crate::atoms;
pub type Result<T> = core::result::Result<T, Error>;

pub enum Error {
    RustlerBadArg,
    RustlerAtom { message: String },
    RustlerRaiseAtom { message: String },
    RustlerRaiseTerm { message: String },
    RustlerTerm { message: String },
    // Lance Errors
    InvalidTableName { name: String, reason: String },
    InvalidInput { message: String },
    TableNotFound { name: String },
    DatabaseNotFound { name: String },
    DatabaseAlreadyExists { name: String },
    IndexNotFound { name: String },
    EmbeddingFunctionNotFound { name: String, reason: String },
    TableAlreadyExists { name: String },
    CreateDir { path: String, message: String },
    Schema { message: String },
    Runtime { message: String },
    ObjectStore { message: String },
    Lance { message: String },
    Arrow { message: String },
    NotSupported { message: String },
    Other { message: String },
}

impl From<RustlerError> for Error {
    fn from(value: RustlerError) -> Self {
        match &value {
            RustlerError::BadArg => Error::RustlerBadArg,
            RustlerError::Atom(msg) => Error::RustlerAtom {
                message: msg.to_string(),
            },
            RustlerError::RaiseAtom(msg) => Error::RustlerRaiseAtom {
                message: msg.to_string(),
            },
            RustlerError::RaiseTerm(_enc) => Error::RustlerRaiseTerm {
                message: format!("{:#?}", value),
            },
            RustlerError::Term(_enc) => Error::RustlerTerm {
                message: format!("{:?}", value),
            },
        }
    }
}

impl From<LanceError> for Error {
    fn from(error: LanceError) -> Self {
        match error {
            LanceError::InvalidTableName { name, reason } => {
                Error::InvalidTableName { name, reason }
            }
            LanceError::InvalidInput { message } => Error::InvalidInput { message },
            LanceError::TableNotFound { name } => Error::TableNotFound { name },
            LanceError::DatabaseNotFound { name } => Error::DatabaseNotFound { name },
            LanceError::DatabaseAlreadyExists { name } => Error::DatabaseAlreadyExists { name },
            LanceError::IndexNotFound { name } => Error::IndexNotFound { name },
            LanceError::EmbeddingFunctionNotFound { name, reason } => {
                Error::EmbeddingFunctionNotFound { name, reason }
            }
            LanceError::TableAlreadyExists { name } => Error::TableAlreadyExists { name },
            LanceError::CreateDir { path, source } => Error::CreateDir {
                path: path,
                message: source.to_string(),
            },
            LanceError::Schema { message } => Error::Schema { message },
            LanceError::Runtime { message } => Error::Runtime { message },
            LanceError::ObjectStore { source } => Error::ObjectStore {
                message: source.to_string(),
            },
            LanceError::Lance { source } => Error::Lance {
                message: source.to_string(),
            },
            LanceError::Arrow { source } => Error::Arrow {
                message: source.to_string(),
            },
            LanceError::NotSupported { message } => Error::NotSupported { message },
            LanceError::Other { message, source: _ } => Error::Other { message: message },
        }
    }
}

impl Encoder for Error {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let error_tuple = match self {
            Error::InvalidTableName { name, reason } => (
                atoms::invalid_table_name(),
                format!("{} is not a valid database name: {}", name, reason),
            ),
            Error::InvalidInput { message } => (atoms::invalid_input(), message.to_string()),
            Error::TableNotFound { name } => (atoms::table_not_found(), name.to_string()),
            Error::DatabaseNotFound { name } => (atoms::database_not_found(), name.to_string()),
            Error::DatabaseAlreadyExists { name } => {
                (atoms::database_already_exists(), name.to_string())
            }
            Error::IndexNotFound { name } => (atoms::index_not_found(), name.to_string()),
            Error::EmbeddingFunctionNotFound { name, reason } => (
                atoms::embedding_function_not_found(),
                format!("Embedding function {} not found: {}", name, reason),
            ),
            Error::TableAlreadyExists { name } => (atoms::table_already_exists(), name.to_string()),
            Error::CreateDir { path, message } => (
                atoms::create_dir(),
                format!("Could not create dir at path {}, reason: {}", path, message),
            ),
            Error::Schema { message } => (atoms::schema(), message.to_string()),
            Error::Runtime { message } => (atoms::runtime(), message.to_string()),
            Error::ObjectStore { message } => (atoms::object_store(), message.to_string()),
            Error::Lance { message } => (atoms::lance(), message.to_string()),
            Error::Arrow { message } => (atoms::arrow(), message.to_string()),
            Error::NotSupported { message } => (atoms::not_supported(), message.to_string()),
            Error::Other { message } => (atoms::other(), message.to_string()),
            Error::RustlerBadArg => (atoms::rustler_bad_arg(), "bad argument".to_string()),
            Error::RustlerAtom { message } => (atoms::rustler_atom(), message.to_string()),
            Error::RustlerRaiseAtom { message } => {
                (atoms::rustler_raise_atom(), message.to_string())
            }
            Error::RustlerRaiseTerm { message } => {
                (atoms::rustler_raise_term(), message.to_string())
            }
            Error::RustlerTerm { message } => (atoms::rustler_term(), message.to_string()),
        };
        error_tuple.encode(env)
    }
}