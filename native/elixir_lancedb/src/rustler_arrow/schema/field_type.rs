use std::sync::Arc;

use arrow_schema::DataType as ArrowDataType;
use rustler::NifTaggedEnum;

use super::child_field::ChildField;

#[derive(NifTaggedEnum, Debug, Clone)]
pub enum FieldType {
    Boolean,
    Utf8,
    Float32,
    Int32,
    Int64,
    List(ChildField),
    FixedSizeList(ChildField, i32),
}

impl From<&ArrowDataType> for FieldType {
    fn from(value: &ArrowDataType) -> Self {
        match value {
            ArrowDataType::Boolean => FieldType::Boolean,
            ArrowDataType::Utf8 => FieldType::Utf8,
            ArrowDataType::Float32 => FieldType::Float32,
            ArrowDataType::Int32 => FieldType::Int32,
            ArrowDataType::Int64 => FieldType::Int64,
            ArrowDataType::List(child_field) => FieldType::List(child_field.into()),
            ArrowDataType::FixedSizeList(child_field, dim) => {
                FieldType::FixedSizeList(child_field.into(), *dim)
            }
            _ => todo!("Data type not implemented for {:?}", value),
        }
    }
}

impl Into<ArrowDataType> for FieldType {
    fn into(self) -> ArrowDataType {
        match self {
            FieldType::Boolean => ArrowDataType::Boolean,
            FieldType::Utf8 => ArrowDataType::Utf8,
            FieldType::Float32 => ArrowDataType::Float32,
            FieldType::Int32 => ArrowDataType::Int32,
            FieldType::Int64 => ArrowDataType::Int64,
            FieldType::List(child_type) => ArrowDataType::List(Arc::new(child_type.into())),
            FieldType::FixedSizeList(child_type, dim) => {
                ArrowDataType::FixedSizeList(Arc::new(child_type.into()), dim)
            }
        }
    }
}
