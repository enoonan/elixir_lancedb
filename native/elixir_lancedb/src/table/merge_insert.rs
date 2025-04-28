use arrow_array::{RecordBatch, RecordBatchIterator};
use rustler::{Decoder, NifResult, ResourceArc, Term};

use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::term_to_arrow::to_arrow,
};

use super::{table_conn, TableResource};

pub struct MergeInsertConfig {
    on: Vec<String>,
    when_matched_update_all: bool,
    when_matched_update_all_filt: Option<String>,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_delete_filt: Option<String>,
}

#[rustler::nif(schedule = "DirtyCpu")]
pub fn merge_insert(
    table: ResourceArc<TableResource>,
    input: Term,
    config: MergeInsertConfig,
) -> Result<()> {
    let table = table_conn(table)?;

    get_runtime().block_on(async {
        let schema = table.schema().await?;
        let columns = to_arrow(input, (*schema).clone())?;
        let ons: Vec<&str> = config.on.iter().map(|s| s.as_str()).collect();
        let mut builder = table.merge_insert(&ons);
        if config.when_matched_update_all {
            builder = builder
                .when_matched_update_all(config.when_matched_update_all_filt)
                .clone();
        }

        if config.when_not_matched_insert_all {
            builder = builder.when_not_matched_insert_all().clone();
        }

        if config.when_not_matched_by_source_delete {
            builder = builder
                .when_not_matched_by_source_delete(config.when_not_matched_by_source_delete_filt)
                .clone();
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| Error::from(e))?;
        let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema);
        builder.execute(Box::new(reader)).await?;
        Ok::<(), Error>(())
    })?;

    Ok(())
}

impl Decoder<'_> for MergeInsertConfig {
    fn decode(term: Term) -> NifResult<Self> {
        let on: Vec<String> = term.map_get(atoms::on()).and_then(|s| s.decode())?;

        let when_matched_update_all: bool = term
            .map_get(atoms::when_matched_update_all())
            .and_then(|s| s.decode())?;

        let when_matched_update_all_filt: Option<String> = term
            .map_get(atoms::when_matched_update_all_filt())
            .ok()
            .and_then(|s| s.decode().ok());

        let when_not_matched_insert_all: bool = term
            .map_get(atoms::when_not_matched_insert_all())
            .and_then(|s| s.decode())?;

        let when_not_matched_by_source_delete: bool = term
            .map_get(atoms::when_not_matched_by_source_delete())
            .and_then(|s| s.decode())?;

        let when_not_matched_by_source_delete_filt: Option<String> = term
            .map_get(atoms::when_not_matched_by_source_delete_filt())
            .ok()
            .and_then(|s| s.decode().ok());

        Ok(MergeInsertConfig {
            on,
            when_matched_update_all,
            when_matched_update_all_filt,
            when_not_matched_insert_all,
            when_not_matched_by_source_delete,
            when_not_matched_by_source_delete_filt,
        })
    }
}
