use lancedb::table::UpdateBuilder;
use rustler::{Decoder, NifResult, NifStruct, ResourceArc, Term};

use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
};

use super::{table_conn, TableResource};

pub struct UpdateConfig {
    pub filter: Option<String>,
    pub columns: Vec<ColumnOperation>,
}

#[derive(NifStruct)]
#[module = "ElixirLanceDB.Native.Table.UpdateConfig.ColumnOperation"]
pub struct ColumnOperation {
    pub column: String,
    pub operation: String,
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn update(table: ResourceArc<TableResource>, update_config: UpdateConfig) -> Result<u64> {
    let table = table_conn(table);
    let result = get_runtime().block_on(async {
        let mut update = table.update();

        update = match update_config.filter {
            Some(filter) => update.only_if(filter),
            None => update,
        };

        let update = update_config.columns.iter().fold(update, |update_acc, op| {
            update_acc.column(&op.column, &op.operation)
        });

        let num_rows_affected = update.execute().await?;

        Ok::<u64, Error>(num_rows_affected)
    })?;

    Ok(result)
}

impl Decoder<'_> for UpdateConfig {
    fn decode(term: Term) -> NifResult<Self> {
        let filter: Option<String> = term
            .map_get(atoms::filter())
            .ok()
            .and_then(|s| s.decode().ok());

        let columns: Vec<ColumnOperation> =
            term.map_get(atoms::columns()).and_then(|s| s.decode())?;

        Ok(UpdateConfig {
            filter: filter,
            columns: columns,
        })
    }
}
