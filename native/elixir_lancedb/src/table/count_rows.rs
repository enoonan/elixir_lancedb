use rustler::ResourceArc;

use crate::{
    error::{Error, Result},
    runtime::get_runtime,
};

use super::{table_conn, TableResource};

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
