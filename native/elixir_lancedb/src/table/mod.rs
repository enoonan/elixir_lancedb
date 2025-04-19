mod query;
use query::QueryRequest;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    error::{Error, Result},
    runtime::get_runtime,
    term_from_arrow,
};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use lancedb::{
    query::{ExecutableQuery, QueryBase},
    Table,
};
use rustler::ResourceArc;
pub struct TableResource(pub Arc<Mutex<Table>>);

#[rustler::nif(schedule = "DirtyCpu")]
fn query<'a>(
    table: ResourceArc<TableResource>,
    query_request: QueryRequest,
) -> Result<Vec<HashMap<String, term_from_arrow::ReturnValue>>> {
    let table = table_conn(table);
    let result: Result<Vec<HashMap<String, term_from_arrow::ReturnValue>>> = get_runtime()
        .block_on(async {
            let schema = table.schema().await?;
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

            term_from_arrow::term_from_arrow(results, schema.as_ref())
        });

    match result {
        Ok(recs) => Ok(recs),
        Err(err) => Err(Error::from(err)),
    }
}

fn table_conn(table_resource: ResourceArc<TableResource>) -> Table {
    let table;
    {
        table = table_resource
            .0
            .lock()
            .expect("Fatal: failed acquiring table resource lock")
            .clone();
    }
    table
}
