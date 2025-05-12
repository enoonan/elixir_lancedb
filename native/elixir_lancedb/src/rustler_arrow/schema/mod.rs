mod child_field;
mod field;
pub mod field_type;

use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
use field::Field;
use rustler::NifStruct;
use std::{collections::HashMap, sync::Arc};

#[derive(NifStruct, Clone)]
#[module = "ElixirLanceDB.Native.Schema"]
pub struct Schema {
    pub fields: Vec<Field>,
    pub metadata: HashMap<String, String>,
}

impl Into<ArrowSchema> for Schema {
    fn into(self) -> ArrowSchema {
        let fields: Vec<ArrowField> = self.fields.iter().map(|f| f.clone().into()).collect();
        arrow_schema::Schema::new(fields)        
    }
}

impl From<Arc<ArrowSchema>> for Schema {
    fn from(value: Arc<ArrowSchema>) -> Self {
        Schema {
            fields: value.fields.iter().map(|f| f.into()).collect(),
            metadata: value.metadata.clone(),
        }
    }
}
