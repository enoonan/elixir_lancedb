use std::{collections::HashMap, sync::{Arc, Mutex}};

use arrow_array::RecordBatch;
use rustler::ResourceArc;
use lancedb::{query::ExecutableQuery, Table};
use crate::{error::{Error, Result}, runtime::get_runtime, term_from_arrow};
use futures::TryStreamExt;
pub struct TableResource(pub Arc<Mutex<Table>>);

#[rustler::nif(schedule = "DirtyCpu")]
fn query<'a>(
    table: ResourceArc<TableResource>,
) -> Result<Vec<HashMap<String, term_from_arrow::Value>>> {
    let table = table_conn(table);

    let result: Result<Vec<HashMap<String, term_from_arrow::Value>>> =
        get_runtime().block_on(async {
            let schema = table.schema().await?;
            let results: Vec<RecordBatch> = table.query().execute().await?.try_collect().await?;

            term_from_arrow::term_from_arrow(results, schema.as_ref())
        });

    match result {
        Ok(recs) => Ok(recs),
        Err(err) => Err(Error::from(err)),
    }
}

fn table_conn(table_resource: ResourceArc<TableResource>) -> Table{
  let table;
  {
    table = table_resource.0.lock().expect("Fatal: failed acquiring table resource lock").clone();
  }
  table
}
