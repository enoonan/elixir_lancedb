use crate::runtime::get_runtime;
use crate::rustler_arrow::term_from_arrow::{from_arrow, ReturnableTerm};
use crate::table::index::DistanceType;
use crate::table::{table_conn, TableResource};
use crate::error::{Error, Result};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::query::{
    ExecutableQuery, Query, QueryBase, QueryExecutionOptions, VectorQuery as LanceVectorQuery,
};
use rustler::{NifStruct, ResourceArc};
use std::collections::HashMap;

use super::plain::QueryRequest;

#[rustler::nif(schedule = "DirtyCpu")]
pub fn vector_search(
    table: ResourceArc<TableResource>,
    request: VectorQueryRequest,
) -> Result<Vec<HashMap<String, ReturnableTerm>>> {
    let table = table_conn(table)?;
    let result: Vec<HashMap<String, ReturnableTerm>> = get_runtime().block_on(async {
        let base_query = request.clone().base.apply_to(table.query());
        let mut vector_query = request.clone().apply_to(base_query)?;

        if request.postfilter {
            vector_query = vector_query.postfilter();
        }

        let record_batch: Vec<RecordBatch> = vector_query.execute().await?.try_collect().await?;
        let results = from_arrow(record_batch)?;
        Ok::<Vec<HashMap<String, ReturnableTerm>>, Error>(results)
    })?;
    Ok(result)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn hybrid_search(
    table: ResourceArc<TableResource>,
    request: VectorQueryRequest,
) -> Result<Vec<HashMap<String, ReturnableTerm>>> {
    let table = table_conn(table)?;
    let results = get_runtime().block_on(async {
        let base_query = request.clone().base.apply_to(table.query());
        let mut vector_query = request.clone().apply_to(base_query)?;

        if request.postfilter {
            vector_query = vector_query.postfilter();
        }

        let record_batch: Vec<RecordBatch> = vector_query
            .execute_hybrid(QueryExecutionOptions::default())
            .await?
            .try_collect()
            .await?;

        let results = from_arrow(record_batch)?;

        Ok::<Vec<HashMap<String, ReturnableTerm>>, Error>(results)
    })?;

    Ok(results)
}

#[derive(NifStruct, Clone)]
#[module = "ElixirLanceDB.Native.Table.VectorQueryRequest"]
pub struct VectorQueryRequest {
    pub base: QueryRequest,
    pub postfilter: bool,

    // Vector
    pub column: Option<String>,
    pub query_vector: Vec<f32>,
    pub nprobes: Option<usize>,
    pub lower_bound: Option<f32>,
    pub upper_bound: Option<f32>,
    pub ef: Option<usize>,
    pub refine_factor: Option<u32>,
    pub distance_type: Option<DistanceType>,
    pub use_index: bool,
}

impl VectorQueryRequest {
    pub fn apply_to(self, query: Query) -> Result<LanceVectorQuery> {
        let mut vector_query = query.nearest_to(self.query_vector)?;

        if let Some(column) = self.column {
            vector_query = vector_query.column(&column);
        }

        if let Some(nprobes) = self.nprobes {
            vector_query = vector_query.nprobes(nprobes);
        }

        vector_query = vector_query.distance_range(self.lower_bound, self.upper_bound);

        if let Some(ef) = self.ef {
            vector_query = vector_query.ef(ef);
        }

        if let Some(refine_factor) = self.refine_factor {
            vector_query = vector_query.refine_factor(refine_factor);
        }

        if let Some(distance_type) = self.distance_type {
            vector_query = vector_query.distance_type(distance_type.into());
        }

        if !self.use_index {
            vector_query = vector_query.bypass_vector_index();
        }

        Ok(vector_query)
    }
}
