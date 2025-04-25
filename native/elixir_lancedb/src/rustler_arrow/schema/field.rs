use std::sync::Arc;

use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
use rustler::NifStruct;

use super::field_type::FieldType;

#[derive(NifStruct, Clone)]
#[module = "ElixirLanceDB.Native.Schema.Field"]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
}

impl From<&Arc<ArrowField>> for Field {
    fn from(value: &Arc<ArrowField>) -> Self {
        let field_type: FieldType = value.data_type().into();
        Field {
            name: value.name().to_string(),
            field_type: field_type,
            nullable: value.is_nullable(),
        }
    }
}

impl Into<ArrowField> for Field {
    fn into(self) -> ArrowField {
        match self.field_type {
            FieldType::Boolean => ArrowField::new(self.name, ArrowDataType::Boolean, self.nullable),
            FieldType::Utf8 => ArrowField::new(self.name, ArrowDataType::Utf8, self.nullable),
            FieldType::Float32 => ArrowField::new(self.name, ArrowDataType::Float32, self.nullable),
            FieldType::Int32 => ArrowField::new(self.name, ArrowDataType::Int32, self.nullable),
            FieldType::Int64 => ArrowField::new(self.name, ArrowDataType::Int64, self.nullable),
            FieldType::List(child) => ArrowField::new(
                self.name,
                ArrowDataType::List(Arc::new(child.into())),
                self.nullable,
            ),
            FieldType::FixedSizeList(child, dimension) => ArrowField::new(
                self.name,
                ArrowDataType::FixedSizeList(Arc::new(child.into()), dimension),
                self.nullable,
            ),
        }
    }
}
