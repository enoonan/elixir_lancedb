use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::term_from_arrow::{from_arrow, ReturnableTerm},
    table::{table_conn, TableResource},
};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::{
    index::scalar::FullTextSearchQuery as LanceFullTextSearchQuery,
    query::{ExecutableQuery, QueryBase},
};
use rustler::{Decoder, NifResult, ResourceArc};
use std::collections::HashMap;

use super::plain::QueryRequest;

#[rustler::nif(schedule = "DirtyCpu")]
pub fn full_text_search(
    table: ResourceArc<TableResource>,
    query: QueryRequest,
) -> Result<Vec<HashMap<String, ReturnableTerm>>> {
    let fts_query = match query.full_text_search {
        None => {
            return Err(Error::InvalidInput {
                message: "full text search query is required".to_string(),
            })
        }
        Some(query) => query,
    };

    let table = table_conn(table)?;
    let result: Vec<HashMap<String, ReturnableTerm>> = get_runtime().block_on(async {
        let query = table.query().full_text_search(fts_query.into());
        let record_batch: Vec<RecordBatch> = query.execute().await?.try_collect().await?;
        let results = from_arrow(record_batch)?;
        Ok::<Vec<HashMap<String, ReturnableTerm>>, Error>(results)
    })?;

    Ok(result)
}

#[derive(Clone, Debug)]
pub struct FullTextSearchQuery {
    pub query: String,
    pub columns: Vec<String>,
    pub limit: Option<i64>,
    pub wand_factor: Option<f32>,
}

impl Decoder<'_> for FullTextSearchQuery {
    fn decode(term: rustler::Term<'_>) -> NifResult<Self> {
        let columns: Vec<String> = term.map_get(atoms::columns())?.decode()?;

        let query: String = term.map_get(atoms::query())?.decode()?;

        let limit: Option<i64> = term
            .map_get(atoms::limit())
            .ok()
            .and_then(|s| s.decode().ok());

        let wand_factor: Option<f32> = term
            .map_get(atoms::wand_factor())
            .ok()
            .and_then(|s| s.decode().ok());

        Ok(FullTextSearchQuery {
            columns,
            query,
            limit,
            wand_factor,
        })
    }
}

impl Into<LanceFullTextSearchQuery> for FullTextSearchQuery {
    fn into(self) -> LanceFullTextSearchQuery {
        LanceFullTextSearchQuery {
            columns: self.columns,
            query: self.query,
            limit: self.limit,
            wand_factor: self.wand_factor,
        }
    }
}
