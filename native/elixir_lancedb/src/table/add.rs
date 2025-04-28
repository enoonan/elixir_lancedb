use crate::{
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::term_to_arrow::to_arrow,
};
use arrow_array::{RecordBatch, RecordBatchIterator};
use rustler::{ResourceArc, Term};

use super::{table_conn, TableResource};

#[rustler::nif(schedule = "DirtyCpu")]
pub fn add(table: ResourceArc<TableResource>, erl_data: Term) -> Result<()> {
    // let arrow_schema = Arc::new(erl_schema.clone().into_arrow());
    let table = table_conn(table)?;
    get_runtime().block_on(async {
        let schema = table.schema().await?;
        let columnar_data = to_arrow(erl_data, (*schema).clone())?;
        let batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(schema.clone(), columnar_data)],
            schema,
        );
        table.add(batches).execute().await?;
        Ok::<(), Error>(())
    })?;
    Ok(())
}
