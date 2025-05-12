use crate::{
    error::Result,
    runtime::get_runtime,
    rustler_arrow::term_from_arrow::{from_arrow, ReturnableTerm},
    table::{table_conn, TableResource},
};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, Query, QueryBase};
use rustler::{NifStruct, ResourceArc};
use std::{collections::HashMap, option::Option};

use super::fts::FullTextSearchQuery;

#[rustler::nif(schedule = "DirtyCpu")]
fn query<'a>(
    table: ResourceArc<TableResource>,
    query_request: QueryRequest,
) -> Result<Vec<HashMap<String, ReturnableTerm>>> {
    let table = table_conn(table)?;

    let result: Vec<HashMap<String, ReturnableTerm>> = get_runtime().block_on(async {
        let query = query_request.apply_to(table.query());
        let results: Vec<RecordBatch> = query.execute().await?.try_collect().await?;
        from_arrow(results)
    })?;

    Ok(result)
}

#[derive(NifStruct, Clone, Debug)]
#[module = "ElixirLanceDB.Native.Table.QueryRequest"]
pub struct QueryRequest {
    pub filter: Option<QueryFilter>,
    pub limit: Option<usize>,
    pub full_text_search: Option<FullTextSearchQuery>,
}

#[derive(NifStruct, Clone, Debug)]
#[module = "ElixirLanceDB.Native.Table.QueryFilter"]
pub struct QueryFilter {
    pub sql: Option<String>,
}

impl QueryRequest {
    pub fn apply_to(self, mut query: Query) -> Query {
        query = match self.filter {
            Some(filter) => match filter.sql {
                Some(sql) => query.only_if(sql),
                None => query,
            },
            None => query,
        };

        query = match self.limit {
            Some(limit) => query.limit(limit),
            None => query,
        };

        query = match self.full_text_search {
            Some(fts) => query.full_text_search(fts.into()),
            None => query,
        };

        query
    }
}
