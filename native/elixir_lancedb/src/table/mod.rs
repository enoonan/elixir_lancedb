mod add;
mod delete;
mod index;
mod merge_insert;
mod optimize;
mod query;
mod update;
mod alter_columns;
mod add_columns;

use std::sync::{Arc, Mutex};

use lancedb::Table;
use rustler::{resource_impl, Resource, ResourceArc};

use crate::{
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::schema::Schema,
};

pub struct TableResource(pub Arc<Mutex<Table>>);

#[resource_impl]
impl Resource for TableResource {}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn count_rows(table: ResourceArc<TableResource>, filter: String) -> Result<usize> {
    let table = table_conn(table);
    let filter = match filter.as_str() {
        "" => None,
        _ => Some(filter),
    };
    let rows = get_runtime().block_on(async {
        let rows = table.count_rows(filter).await?;
        Ok::<usize, Error>(rows)
    })?;
    Ok(rows)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn schema(table: ResourceArc<TableResource>) -> Result<Schema> {
    let table = table_conn(table);
    let schema = get_runtime().block_on(async {
        let s: Schema = table.schema().await?.into();
        Ok::<Schema, Error>(s)
    })?;

    Ok(schema)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn drop_columns(table: ResourceArc<TableResource>, columns: Vec<&str>) -> Result<()> {
    let table = table_conn(table);
    get_runtime().block_on(async {
        table.drop_columns(&columns).await?;
        Ok::<(), Error>(())
    })
}

fn table_conn(table: ResourceArc<TableResource>) -> Table {
    let result;
    {
        result = table
            .0
            .lock()
            .expect("Fatal: failed acquiring table resource lock")
            .clone();
    }
    result
}
