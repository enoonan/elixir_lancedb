use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::term_from_arrow::{from_arrow, ReturnableTerm},
    table::{table_conn, TableResource},
};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use rustler::{Decoder, NifResult, ResourceArc, Term};
use std::{collections::HashMap, option::Option};

#[rustler::nif(schedule = "DirtyCpu")]
fn query<'a>(
    table: ResourceArc<TableResource>,
    query_request: QueryRequest,
) -> Result<Vec<HashMap<String, ReturnableTerm>>> {
    let table = table_conn(table);
    let result: Result<Vec<HashMap<String, ReturnableTerm>>> = get_runtime().block_on(async {
        let mut query = table.query();
        query = match query_request.filter {
            Some(filter) => match filter.sql {
                Some(sql) => query.only_if(sql),
                None => query,
            },
            None => query,
        };

        query = match query_request.limit {
            Some(limit) => query.limit(limit),
            None => query,
        };

        let results: Vec<RecordBatch> = query.execute().await?.try_collect().await?;

        from_arrow(results)
    });

    match result {
        Ok(recs) => Ok(recs),
        Err(err) => Err(Error::from(err)),
    }
}

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
