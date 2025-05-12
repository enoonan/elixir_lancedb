use crate::{
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::term_from_arrow::{from_arrow, ReturnableTerm},
    table::{table_conn, TableResource},
};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::{
    index::scalar::{FtsQuery, FullTextSearchQuery as LanceFullTextSearchQuery, MatchQuery},
    query::{ExecutableQuery, QueryBase},
};
use rustler::{NifStruct, ResourceArc};
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

#[derive(NifStruct, Clone, Debug)]
#[module = "ElixirLanceDB.Native.Table.FullTextSearchQueryRequest"]
pub struct FullTextSearchQuery {
    pub query: String,
    pub columns: Vec<String>,
    pub limit: Option<i64>,
    pub wand_factor: Option<f32>,
}

impl Into<LanceFullTextSearchQuery> for FullTextSearchQuery {
    fn into(self) -> LanceFullTextSearchQuery {
        LanceFullTextSearchQuery {
            // columns: self.columns,
            query: FtsQuery::Match(
                MatchQuery::new(self.query).with_column(Some(self.columns[0].clone())),
            ),
            limit: self.limit,
            wand_factor: self.wand_factor,
        }
    }
}
