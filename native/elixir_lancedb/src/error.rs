use crate::atoms::{self};
use arrow_schema::ArrowError;
use lancedb::Error as LanceError;
use rustler::{Encoder, Env, Error as RustlerError, Term};
pub type Result<T> = core::result::Result<T, Error>;

pub enum Error {
    InvalidInput { message: String },
    Other { message: String },

    // Rustler Errors
    RustlerBadArg,
    RustlerAtom { message: String },
    RustlerRaiseAtom { message: String },
    RustlerRaiseTerm { message: String },
    RustlerTerm { message: String },

    // Lance Errors
    LanceInvalidTableName { name: String, reason: String },
    LanceInvalidInput { message: String },
    LanceTableNotFound { name: String },
    LanceDatabaseNotFound { name: String },
    LanceDatabaseAlreadyExists { name: String },
    LanceIndexNotFound { name: String },
    LanceEmbeddingFunctionNotFound { name: String, reason: String },
    LanceTableAlreadyExists { name: String },
    LanceCreateDir { path: String, message: String },
    LanceSchema { message: String },
    LanceRuntime { message: String },
    LanceObjectStore { message: String },
    Lance { message: String },
    LanceArrow { message: String },
    LanceNotSupported { message: String },
    LanceOther { message: String },

    // Arrow Errors
    ArrowNotYetImplemented { message: String },
    ArrowExternalError { message: String },
    ArrowCastError { message: String },
    ArrowMemoryError { message: String },
    ArrowParseError { message: String },
    ArrowSchemaError { messsage: String },
    ArrowComputeError { message: String },
    ArrowDivideByZero,
    ArrowArithmeticOverflow { message: String },
    ArrowCsvError { message: String },
    ArrowJsonError { message: String },
    ArrowIoError { message: String, error: String },
    ArrowIpcError { message: String },
    ArrowInvalidArgumentError { message: String },
    ArrowParquetError { message: String },
    ArrowCDataInterface { message: String },
    ArrowDictionaryKeyOverflowError,
    ArrowRunEndIndexOverflowError,
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
                Error::LanceInvalidTableName { name, reason }
            }
            LanceError::InvalidInput { message } => Error::LanceInvalidInput { message },
            LanceError::TableNotFound { name } => Error::LanceTableNotFound { name },
            LanceError::DatabaseNotFound { name } => Error::LanceDatabaseNotFound { name },
            LanceError::DatabaseAlreadyExists { name } => {
                Error::LanceDatabaseAlreadyExists { name }
            }
            LanceError::IndexNotFound { name } => Error::LanceIndexNotFound { name },
            LanceError::EmbeddingFunctionNotFound { name, reason } => {
                Error::LanceEmbeddingFunctionNotFound { name, reason }
            }
            LanceError::TableAlreadyExists { name } => Error::LanceTableAlreadyExists { name },
            LanceError::CreateDir { path, source } => Error::LanceCreateDir {
                path: path,
                message: source.to_string(),
            },
            LanceError::Schema { message } => Error::LanceSchema { message },
            LanceError::Runtime { message } => Error::LanceRuntime { message },
            LanceError::ObjectStore { source } => Error::LanceObjectStore {
                message: source.to_string(),
            },
            LanceError::Lance { source } => Error::Lance {
                message: source.to_string(),
            },
            LanceError::Arrow { source } => Error::LanceArrow {
                message: source.to_string(),
            },
            LanceError::NotSupported { message } => Error::LanceNotSupported { message },
            LanceError::Other { message, source: _ } => Error::LanceOther { message: message },
        }
    }
}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        match value {
            ArrowError::NotYetImplemented(msg) => Error::ArrowNotYetImplemented { message: msg },
            ArrowError::ExternalError(error) => Error::ArrowExternalError {
                message: error.to_string(),
            },
            ArrowError::CastError(msg) => Error::ArrowCastError { message: msg },
            ArrowError::MemoryError(msg) => Error::ArrowMemoryError { message: msg },
            ArrowError::ParseError(msg) => Error::ArrowParseError { message: msg },
            ArrowError::SchemaError(msg) => Error::ArrowSchemaError { messsage: msg },
            ArrowError::ComputeError(msg) => Error::ArrowComputeError { message: msg },
            ArrowError::DivideByZero => Error::ArrowDivideByZero,
            ArrowError::ArithmeticOverflow(msg) => Error::ArrowArithmeticOverflow { message: msg },
            ArrowError::CsvError(msg) => Error::ArrowCsvError { message: msg },
            ArrowError::JsonError(msg) => Error::ArrowJsonError { message: msg },
            ArrowError::IoError(msg, error) => Error::ArrowIoError {
                message: msg,
                error: error.to_string(),
            },
            ArrowError::IpcError(msg) => Error::ArrowIpcError { message: msg },
            ArrowError::InvalidArgumentError(msg) => {
                Error::ArrowInvalidArgumentError { message: msg }
            }
            ArrowError::ParquetError(msg) => Error::ArrowParquetError { message: msg },
            ArrowError::CDataInterface(msg) => Error::ArrowCDataInterface { message: msg },
            ArrowError::DictionaryKeyOverflowError => Error::ArrowDictionaryKeyOverflowError,
            ArrowError::RunEndIndexOverflowError => Error::ArrowRunEndIndexOverflowError,
        }
    }
}

// Add this implementation for String errors
impl From<String> for Error {
    fn from(message: String) -> Self {
        Error::Other { message }
    }
}

impl Encoder for Error {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        let error_tuple = match self {
            Error::Other { message } => {
                (atoms::error(), (atoms::lance_other(), message.to_string()))
            }
            Error::InvalidInput { message } => (
                atoms::error(),
                (atoms::invalid_input(), message.to_string()),
            ),
            // Lance
            Error::LanceInvalidTableName { name, reason } => (
                atoms::error(),
                (
                    atoms::lance_invalid_table_name(),
                    format!("{} is not a valid database name: {}", name, reason),
                ),
            ),
            Error::LanceInvalidInput { message } => (
                atoms::error(),
                (atoms::lance_invalid_input(), message.to_string()),
            ),
            Error::LanceTableNotFound { name } => (
                atoms::error(),
                (atoms::lance_table_not_found(), name.to_string()),
            ),
            Error::LanceDatabaseNotFound { name } => (
                atoms::error(),
                (atoms::lance_database_not_found(), name.to_string()),
            ),
            Error::LanceDatabaseAlreadyExists { name } => (
                atoms::error(),
                (atoms::lance_database_already_exists(), name.to_string()),
            ),
            Error::LanceIndexNotFound { name } => (
                atoms::error(),
                (atoms::lance_index_not_found(), name.to_string()),
            ),
            Error::LanceEmbeddingFunctionNotFound { name, reason } => (
                atoms::error(),
                (
                    atoms::lance_embedding_function_not_found(),
                    format!("Embedding function {} not found: {}", name, reason),
                ),
            ),
            Error::LanceTableAlreadyExists { name } => (
                atoms::error(),
                (atoms::lance_table_already_exists(), name.to_string()),
            ),
            Error::LanceCreateDir { path, message } => (
                atoms::error(),
                (
                    atoms::lance_create_dir(),
                    format!("Could not create dir at path {}, reason: {}", path, message),
                ),
            ),
            Error::LanceSchema { message } => {
                (atoms::error(), (atoms::lance_schema(), message.to_string()))
            }
            Error::LanceRuntime { message } => (
                atoms::error(),
                (atoms::lance_runtime(), message.to_string()),
            ),
            Error::LanceObjectStore { message } => (
                atoms::error(),
                (atoms::lance_object_store(), message.to_string()),
            ),
            Error::Lance { message } => (atoms::error(), (atoms::lance(), message.to_string())),
            Error::LanceArrow { message } => {
                (atoms::error(), (atoms::lance_arrow(), message.to_string()))
            }
            Error::LanceNotSupported { message } => (
                atoms::error(),
                (atoms::lance_not_supported(), message.to_string()),
            ),
            Error::LanceOther { message } => {
                (atoms::error(), (atoms::lance_other(), message.to_string()))
            }

            // Rustler
            Error::RustlerBadArg => (
                atoms::error(),
                (atoms::rustler_bad_arg(), "bad argument".to_string()),
            ),
            Error::RustlerAtom { message } => {
                (atoms::error(), (atoms::rustler_atom(), message.to_string()))
            }
            Error::RustlerRaiseAtom { message } => (
                atoms::error(),
                (atoms::rustler_raise_atom(), message.to_string()),
            ),
            Error::RustlerRaiseTerm { message } => (
                atoms::error(),
                (atoms::rustler_raise_term(), message.to_string()),
            ),
            Error::RustlerTerm { message } => {
                (atoms::error(), (atoms::rustler_term(), message.to_string()))
            }

            // Arrow
            Error::ArrowNotYetImplemented { message } => (
                atoms::error(),
                (atoms::arrow_not_yet_implemented(), message.to_string()),
            ),
            Error::ArrowExternalError { message } => (
                atoms::error(),
                (atoms::arrow_external_error(), message.to_string()),
            ),
            Error::ArrowCastError { message } => (
                atoms::error(),
                (atoms::arrow_cast_error(), message.to_string()),
            ),
            Error::ArrowMemoryError { message } => (
                atoms::error(),
                (atoms::arrow_memory_error(), message.to_string()),
            ),
            Error::ArrowParseError { message } => (
                atoms::error(),
                (atoms::arrow_parse_error(), message.to_string()),
            ),
            Error::ArrowSchemaError { messsage } => (
                atoms::error(),
                (atoms::arrow_schema_error(), messsage.to_string()),
            ),
            Error::ArrowComputeError { message } => (
                atoms::error(),
                (atoms::arrow_compute_error(), message.to_string()),
            ),
            Error::ArrowDivideByZero => (
                atoms::error(),
                (
                    atoms::arrow_divide_by_zero(),
                    "no further information provided".to_string(),
                ),
            ),
            Error::ArrowArithmeticOverflow { message } => (
                atoms::error(),
                (atoms::arrow_arithmetic_overflow(), message.to_string()),
            ),
            Error::ArrowCsvError { message } => (
                atoms::error(),
                (atoms::arrow_csv_error(), message.to_string()),
            ),
            Error::ArrowJsonError { message } => (
                atoms::error(),
                (atoms::arrow_json_error(), message.to_string()),
            ),
            Error::ArrowIoError { message, error } => (
                atoms::error(),
                (atoms::arrow_io_error(), format!("{}. {}.", message, error)),
            ),
            Error::ArrowIpcError { message } => (
                atoms::error(),
                (atoms::arrow_ipc_error(), message.to_string()),
            ),
            Error::ArrowInvalidArgumentError { message } => (
                atoms::error(),
                (atoms::arrow_invalid_argument_error(), message.to_string()),
            ),
            Error::ArrowParquetError { message } => (
                atoms::error(),
                (atoms::arrow_parquet_error(), message.to_string()),
            ),
            Error::ArrowCDataInterface { message } => (
                atoms::error(),
                (atoms::arrow_cdata_interface(), message.to_string()),
            ),
            Error::ArrowDictionaryKeyOverflowError => (
                atoms::error(),
                (
                    atoms::arrow_dictionary_key_overflow_error(),
                    "no further information provided".to_string(),
                ),
            ),
            Error::ArrowRunEndIndexOverflowError => (
                atoms::error(),
                (
                    atoms::arrow_run_end_index_overflow_error(),
                    "no further information provided".to_string(),
                ),
            ),
        };
        error_tuple.encode(env)
    }
}
