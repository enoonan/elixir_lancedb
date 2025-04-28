mod add;
mod add_columns;
mod alter_columns;
mod delete;
mod index;
mod merge_insert;
mod optimize;
mod query;
mod update;

use std::sync::{Arc, Mutex};

use lancedb::Table;
use rustler::{resource_impl, Resource, ResourceArc};

use crate::{
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::schema::Schema,
};

pub struct TableResource(pub Arc<Mutex<Option<Table>>>);

#[resource_impl]
impl Resource for TableResource {}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn close_table(table: ResourceArc<TableResource>) -> Result<()> {
    let mut lock = table.0.lock().map_err(|_| Error::TableMutexLockPoisoned {
        message: "failed closing table".to_string(),
    })?;
    *lock = None;
    Ok(())
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn count_rows(table: ResourceArc<TableResource>, filter: String) -> Result<usize> {
    let table = table_conn(table)?;
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
    let table = table_conn(table)?;
    let schema = get_runtime().block_on(async {
        let s: Schema = table.schema().await?.into();
        Ok::<Schema, Error>(s)
    })?;

    Ok(schema)
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn drop_columns(table: ResourceArc<TableResource>, columns: Vec<&str>) -> Result<()> {
    let table = table_conn(table)?;
    get_runtime().block_on(async {
        table.drop_columns(&columns).await?;
        Ok::<(), Error>(())
    })
}

fn table_conn(table: ResourceArc<TableResource>) -> Result<Table> {
    let result;
    {
        result = table
            .0
            .lock()
            .expect("Fatal: failed acquiring table resource lock")
            .clone();
    }
    result.ok_or_else(|| Error::TableConnectionClosed {
        message: "the table connection is not open".to_string(),
    })
}
