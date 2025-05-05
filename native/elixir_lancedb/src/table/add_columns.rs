use std::sync::Arc;

use lancedb::table::NewColumnTransform as LanceNewColumnTransform;
// use lance::dataset::schema_evolution::NewColumnTransform as LanceNewColumnTransform;
use rustler::{Decoder, ResourceArc};

use crate::{
    atoms,
    error::{Error, Result},
    runtime::get_runtime,
    rustler_arrow::schema::Schema,
};

use super::{table_conn, TableResource};

#[rustler::nif(schedule = "DirtyCpu")]
fn add_columns(table: ResourceArc<TableResource>, transforms: NewColumnTransform) -> Result<()> {
    let table = table_conn(table)?;
    let result = get_runtime().block_on(async {
        let result = table.add_columns(transforms.into(), None).await?;

        Ok::<(), Error>(result)
    })?;

    Ok(result)
}

pub enum NewColumnTransform {
    AllNulls(Schema),
}

impl Decoder<'_> for NewColumnTransform {
    fn decode(term: rustler::Term<'_>) -> rustler::NifResult<Self> {
        let transform_type = term.map_get(atoms::transform_type())?.atom_to_string()?;
        match transform_type.as_str() {
            "all_nulls" => {
                let schema: Schema = term.map_get(atoms::schema())?.decode()?;
                Ok(NewColumnTransform::AllNulls(schema))
            }
            _ => todo!("transform_type {} not supported", transform_type),
        }
    }
}

impl Into<LanceNewColumnTransform> for NewColumnTransform {
    fn into(self) -> LanceNewColumnTransform {
        match self {
            NewColumnTransform::AllNulls(schema) => {
                LanceNewColumnTransform::AllNulls(Arc::new(schema.into()))
            }
        }
    }
}
