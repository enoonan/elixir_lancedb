use rustler::{Decoder, NifResult, Term};
use std::option::Option;

use crate::atoms;
#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub filter: Option<QueryFilter>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct QueryFilter {
    pub sql: Option<String>,
}

impl Decoder<'_> for QueryFilter {
    fn decode(term: Term) -> NifResult<Self> {
        let sql: Option<String> = term
            .map_get(atoms::sql())
            .ok()
            .and_then(|s| s.decode().ok());

        Ok(QueryFilter { sql })
    }
}

impl Decoder<'_> for QueryRequest {
    fn decode(term: Term) -> NifResult<Self> {
        let filter: Option<QueryFilter> = term
            .map_get(atoms::filter())
            .ok()
            .and_then(|filter| filter.decode().ok());

        let limit: Option<usize> = term
            .map_get(atoms::limit())
            .ok()
            .and_then(|limit| limit.decode().ok().into());

        Ok(QueryRequest { filter, limit })
    }
}
