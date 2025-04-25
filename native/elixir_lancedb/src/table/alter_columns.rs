use crate::{
    error::{Error, Result},
    runtime::get_runtime,
};

use super::{super::rustler_arrow::schema::field_type::FieldType, table_conn, TableResource};
use lance::dataset::ColumnAlteration as LanceColumnAlteration;
use rustler::{NifStruct, ResourceArc};

#[rustler::nif(schedule = "DirtyCpu")]
pub fn alter_columns(
    table: ResourceArc<TableResource>,
    alterations: Vec<ColumnAlteration>,
) -> Result<()> {
    let table = table_conn(table);
    let result = get_runtime().block_on(async {
        let lance_alterations: Vec<LanceColumnAlteration> =
            alterations.into_iter().map(|a| a.into()).collect();
        Ok::<(), Error>(table.alter_columns(&lance_alterations).await?)
    })?;

    Ok(result)
}

#[derive(NifStruct)]
#[module = "ElixirLanceDB.Native.Schema.ColumnAlteration"]
pub struct ColumnAlteration {
    pub path: String,
    pub rename: Option<String>,
    pub nullable: Option<bool>,
    pub data_type: Option<FieldType>,
}

impl Into<LanceColumnAlteration> for ColumnAlteration {
    fn into(self) -> LanceColumnAlteration {
        LanceColumnAlteration {
            path: self.path,
            rename: self.rename,
            nullable: self.nullable,
            data_type: match self.data_type {
                Some(field_type) => Some(field_type.into()),
                None => None,
            },
        }
    }
}
