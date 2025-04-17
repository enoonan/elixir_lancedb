use arrow_schema::{DataType, Field as ArrowField};
use rustler::{NifStruct, NifTaggedEnum, NifUnitEnum};
use std::{collections::HashMap, sync::Arc};

#[derive(NifStruct, Clone)]
#[module = "ElixirLanceDB.Native.Schema.Field"]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub nullable: bool,
}

impl Field {
    fn into_arrow(self) -> ArrowField {
        match self.field_type {
            FieldType::Utf8 => ArrowField::new(self.name, DataType::Utf8, self.nullable),
            FieldType::Float32 => ArrowField::new(self.name, DataType::Float32, self.nullable),
            FieldType::Int32 => ArrowField::new(self.name, DataType::Int32, self.nullable),
            FieldType::List(child) => ArrowField::new(
                self.name,
                DataType::List(Arc::new(child.into_arrow())),
                self.nullable,
            ),
            FieldType::FixedSizeList(child, dimension) => ArrowField::new(
                self.name,
                DataType::FixedSizeList(Arc::new(child.into_arrow()), dimension),
                self.nullable,
            ),
        }
    }
}

#[derive(NifStruct, Clone, Debug)]
#[module = "ElixirLanceDB.Native.Schema.Field"]
pub struct ChildField {
    pub name: String,
    pub field_type: ChildFieldType,
    pub nullable: bool,
}

impl ChildField {
    fn into_arrow(self) -> ArrowField {
        match self.field_type {
            ChildFieldType::Utf8 => ArrowField::new(self.name, DataType::Utf8, self.nullable),
            ChildFieldType::Float32 => ArrowField::new(self.name, DataType::Float32, self.nullable),
            ChildFieldType::Int32 => ArrowField::new(self.name, DataType::Int32, self.nullable),
        }
    }
}

#[derive(NifTaggedEnum, Debug, Clone)]
pub enum FieldType {
    Utf8,
    Float32,
    Int32,
    List(ChildField),
    FixedSizeList(ChildField, i32),
}

#[derive(NifUnitEnum, Clone, Copy, Debug)]
pub enum ChildFieldType {
    Utf8,
    Float32,
    Int32,
}

#[derive(NifStruct, Clone)]
#[module = "ElixirLanceDB.Native.Schema"]
pub struct Schema {
    pub fields: Vec<Field>,
    pub metadata: HashMap<String, String>,
}

impl Schema {
    pub fn into_arrow(self) -> arrow_schema::Schema {
        let fields: Vec<ArrowField> = self.fields.iter().map(|f| f.clone().into_arrow()).collect();
        arrow_schema::Schema::new(fields)
    }
}
