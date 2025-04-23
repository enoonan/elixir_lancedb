use std::collections::HashMap;

use crate::runtime::get_runtime;
use crate::rustler_arrow::term_from_arrow::{from_arrow, ReturnableTerm};
use crate::table::index::DistanceType;
use crate::table::{table_conn, TableResource};
use crate::{
    atoms,
    error::{Error, Result},
};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, Query, QueryBase, VectorQuery as LanceVectorQuery};
use rustler::{Decoder, NifResult, ResourceArc};

#[rustler::nif(schedule = "DirtyCpu")]
pub fn vector_search(
    table: ResourceArc<TableResource>,
    request: VectorQueryRequest,
) -> Result<Vec<HashMap<String, ReturnableTerm>>> {
    let table = table_conn(table);
    let result: Vec<HashMap<String, ReturnableTerm>> = get_runtime().block_on(async {
        let mut query = table.query();

        if let Some(limit) = &request.limit {
            query = query.limit(*limit);
        }

        if let Some(filter) = &request.filter {
            query = query.only_if(filter);
        }

        let mut vector_query = request.clone().apply_to(query)?;
        if request.postfilter {
            vector_query = vector_query.postfilter();
        }
        let record_batch: Vec<RecordBatch> = vector_query.execute().await?.try_collect().await?;
        let results = from_arrow(record_batch)?;
        Ok::<Vec<HashMap<String, ReturnableTerm>>, Error>(results)
    })?;
    Ok(result)
}

#[derive(Clone)]
pub struct VectorQueryRequest {
    // base querying
    pub limit: Option<usize>,
    pub filter: Option<String>,

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

impl Decoder<'_> for VectorQueryRequest {
    fn decode(term: rustler::Term<'_>) -> NifResult<Self> {
        let limit: Option<usize> = term
            .map_get(atoms::column())
            .ok()
            .and_then(|s| s.decode().ok());

        let filter: Option<String> = term
            .map_get(atoms::filter())
            .ok()
            .and_then(|s| s.decode().ok());

        let postfilter: bool = term.map_get(atoms::postfilter())?.decode()?;

        // Extract column as an optional string
        let column: Option<String> = term
            .map_get(atoms::column())
            .ok()
            .and_then(|s| s.decode().ok());

        // Extract query_vector as a required Vec<f32>
        let query_vector: Vec<f32> = term.map_get(atoms::query_vector())?.decode()?;

        // Extract optional numeric parameters
        let nprobes: Option<usize> = term
            .map_get(atoms::nprobes())
            .ok()
            .and_then(|s| s.decode().ok());

        let lower_bound: Option<f32> = term
            .map_get(atoms::lower_bound())
            .ok()
            .and_then(|s| s.decode().ok());

        let upper_bound: Option<f32> = term
            .map_get(atoms::upper_bound())
            .ok()
            .and_then(|s| s.decode().ok());

        let ef: Option<usize> = term.map_get(atoms::ef()).ok().and_then(|s| s.decode().ok());

        let refine_factor: Option<u32> = term
            .map_get(atoms::refine_factor())
            .ok()
            .and_then(|s| s.decode().ok());

        // Extract optional distance_type enum
        let distance_type: Option<DistanceType> = term
            .map_get(atoms::distance_type())
            .ok()
            .and_then(|s| s.decode().ok());

        // Extract boolean parameter
        let use_index: bool = term
            .map_get(atoms::use_index())
            .map_or(true, |val| val.decode().unwrap_or(true)); // Default to true if missing or decode fails

        Ok(VectorQueryRequest {
            limit,
            filter,
            postfilter,
            column,
            query_vector,
            nprobes,
            lower_bound,
            upper_bound,
            ef,
            refine_factor,
            distance_type,
            use_index,
        })
    }
}
impl VectorQueryRequest {
    fn apply_to(self, query: Query) -> Result<LanceVectorQuery> {
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
