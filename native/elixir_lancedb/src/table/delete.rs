use rustler::ResourceArc;

use crate::{error::Result, runtime::get_runtime};

use super::{table_conn, TableResource};

#[rustler::nif(schedule = "DirtyCpu")]
pub fn delete(table: ResourceArc<TableResource>, predicate: String) -> Result<()> {
    let table = table_conn(table);

    get_runtime().block_on(async { table.delete(predicate.as_str()).await })?;

    Ok(())
}
