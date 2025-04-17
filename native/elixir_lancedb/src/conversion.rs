use arrow_array::{
    builder::{
        ArrayBuilder, FixedSizeListBuilder, Float32Builder, Int32Builder, ListBuilder,
        StringBuilder,
    },
    ArrayRef,
};
use rustler::Term;

use crate::schema::{ChildFieldType, FieldType, Schema};

pub fn to_arrow(term: Term, erl_schema: Schema) -> Result<Vec<ArrayRef>, ()> {
    if !term.is_list() {
        return Err(());
    }

    let empty_cols: Vec<Box<dyn ArrayBuilder>> =
        erl_schema
            .clone()
            .fields
            .into_iter()
            .fold(vec![], |mut acc, field| {
                let builder: Box<dyn ArrayBuilder> = match field.field_type {
                    FieldType::Utf8 => Box::new(StringBuilder::new()),
                    FieldType::Float32 => Box::new(Float32Builder::new()),
                    FieldType::Int32 => Box::new(Int32Builder::new()),
                    FieldType::List(child) => match child.field_type {
                        ChildFieldType::Utf8 => {
                            Box::new(ListBuilder::<StringBuilder>::new(StringBuilder::new()))
                        }
                        ChildFieldType::Float32 => {
                            Box::new(ListBuilder::<Float32Builder>::new(Float32Builder::new()))
                        }
                        ChildFieldType::Int32 => {
                            Box::new(ListBuilder::<Int32Builder>::new(Int32Builder::new()))
                        }
                    },
                    FieldType::FixedSizeList(child, dimension) => match child.field_type {
                        ChildFieldType::Utf8 => {
                            Box::new(FixedSizeListBuilder::<StringBuilder>::new(
                                StringBuilder::new(),
                                dimension,
                            ))
                        }
                        ChildFieldType::Float32 => {
                            Box::new(FixedSizeListBuilder::<Float32Builder>::new(
                                Float32Builder::new(),
                                dimension,
                            ))
                        }
                        ChildFieldType::Int32 => {
                            Box::new(FixedSizeListBuilder::<Int32Builder>::new(
                                Int32Builder::new(),
                                dimension,
                            ))
                        }
                    },
                };
                acc.push(builder);
                acc
            });

    let mut builders: Vec<Box<dyn ArrayBuilder>> =
        term.into_list_iterator()
            .unwrap()
            .fold(empty_cols, |mut acc, record| {
                for (idx, field) in erl_schema.clone().fields.into_iter().enumerate() {
                    let val = record.map_get(field.name).unwrap();
                    match field.field_type {
                        FieldType::Utf8 => {
                            if let Some(builder) =
                                acc[idx].as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                let the_str: String = val.decode().unwrap();
                                builder.append_value(the_str);
                            }
                        }
                        FieldType::Int32 => {
                            if let Some(builder) =
                                acc[idx].as_any_mut().downcast_mut::<Int32Builder>()
                            {
                                let the_int: i32 = val.decode().unwrap();
                                builder.append_value(the_int);
                            }
                        }
                        FieldType::Float32 => {
                            if let Some(builder) =
                                acc[idx].as_any_mut().downcast_mut::<Float32Builder>()
                            {
                                let the_float: f32 = val.decode().unwrap();
                                builder.append_value(the_float);
                            }
                        }
                        FieldType::List(child) => match child.field_type {
                            ChildFieldType::Utf8 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<StringBuilder>>()
                                {
                                    let the_list: Vec<String> = val.decode().unwrap();
                                    for s in the_list.iter() {
                                        builder.values().append_value(s.as_str());
                                    }
                                    builder.append(true);
                                }
                            }
                            ChildFieldType::Int32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<Int32Builder>>()
                                {
                                    let the_list: Vec<i32> = val.decode().unwrap();
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            ChildFieldType::Float32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<Float32Builder>>()
                                {
                                    let the_list: Vec<f32> = val.decode().unwrap();

                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                        },
                        FieldType::FixedSizeList(child, _dim) => match child.field_type {
                            ChildFieldType::Utf8 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<FixedSizeListBuilder<StringBuilder>>()
                                {
                                    let the_list: Vec<String> = val.decode().unwrap();
                                    for s in the_list.iter() {
                                        builder.values().append_value(s.as_str());
                                    }
                                    builder.append(true);
                                }
                            }
                            ChildFieldType::Int32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<FixedSizeListBuilder<Int32Builder>>()
                                {
                                    let the_list: Vec<i32> = val.decode().unwrap();
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            ChildFieldType::Float32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<FixedSizeListBuilder<Float32Builder>>()
                                {
                                    let the_list: Vec<f32> = val.decode().unwrap();
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                        },
                    };
                }
                acc
            });

    let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
    Ok(columns)
}
