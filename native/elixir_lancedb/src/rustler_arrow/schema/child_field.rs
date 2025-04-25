use std::sync::Arc;

use arrow_schema::{DataType as ArrowDataType, Field as ArrowField};
use rustler::{NifStruct, NifUnitEnum};

#[derive(NifStruct, Clone, Debug)]
#[module = "ElixirLanceDB.Native.Schema.Field"]

// This module represents a subset of Field which includes only primitive values
// It may be deprecatable if we crack nested lists 
// and recursive ser/de for arrow Schema and RecordBatch

pub struct ChildField {
    pub name: String,
    pub field_type: ChildFieldType,
    pub nullable: bool,
}

#[derive(NifUnitEnum, Clone, Copy, Debug)]
pub enum ChildFieldType {
    Boolean,
    Utf8,
    Float32,
    Int32,
    Int64,
}

impl From<&Arc<ArrowField>> for ChildField {
    fn from(value: &Arc<ArrowField>) -> Self {
        let field_type: ChildFieldType = value.data_type().into();
        ChildField {
            name: value.name().to_string(),
            field_type: field_type,
            nullable: value.is_nullable(),
        }
    }
}

impl Into<ArrowField> for ChildField {
    fn into(self) -> ArrowField {
        match self.field_type {
            ChildFieldType::Boolean => {
                ArrowField::new(self.name, ArrowDataType::Boolean, self.nullable)
            }
            ChildFieldType::Utf8 => ArrowField::new(self.name, ArrowDataType::Utf8, self.nullable),
            ChildFieldType::Float32 => {
                ArrowField::new(self.name, ArrowDataType::Float32, self.nullable)
            }
            ChildFieldType::Int32 => {
                ArrowField::new(self.name, ArrowDataType::Int32, self.nullable)
            },
           ChildFieldType::Int64 => {
                ArrowField::new(self.name, ArrowDataType::Int64, self.nullable)
            } 
        }
    }
}

impl From<&ArrowDataType> for ChildFieldType {
    fn from(value: &ArrowDataType) -> Self {
        match value {
            ArrowDataType::Boolean => ChildFieldType::Boolean,
            ArrowDataType::Utf8 => ChildFieldType::Utf8,
            ArrowDataType::Float32 => ChildFieldType::Float32,
            ArrowDataType::Int32 => ChildFieldType::Int32,
            ArrowDataType::Int64 => ChildFieldType::Int64,
            _ => todo!("Data type not implemented for {:?}", value),
        }
    }
}
