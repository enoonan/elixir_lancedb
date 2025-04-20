use crate::error::{Error, Result};
use arrow_array::{
    builder::{
        ArrayBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Int32Builder,
        ListBuilder, StringBuilder,
    },
    ArrayRef,
};
use arrow_schema::DataType;
use rustler::Term;

pub fn to_arrow(term: Term, schema: arrow_schema::Schema) -> Result<Vec<ArrayRef>> {
    if !term.is_list() {
        return Err(Error::InvalidInput {
            message: format!("Expected list term, got: {:?}", term.get_type()),
        });
    }

    let empty_cols: Vec<Box<dyn ArrayBuilder>> =
        schema
            .clone()
            .fields()
            .into_iter()
            .fold(vec![], |mut acc, field| {
                let builder: Box<dyn ArrayBuilder> = match field.data_type() {
                    DataType::Boolean => Box::new(BooleanBuilder::new()),
                    DataType::Utf8 => Box::new(StringBuilder::new()),
                    DataType::Float32 => Box::new(Float32Builder::new()),
                    DataType::Int32 => Box::new(Int32Builder::new()),
                    DataType::List(child) => match child.data_type() {
                        DataType::Boolean => {
                            Box::new(ListBuilder::<BooleanBuilder>::new(BooleanBuilder::new()))
                        }
                        DataType::Utf8 => {
                            Box::new(ListBuilder::<StringBuilder>::new(StringBuilder::new()))
                        }
                        DataType::Float32 => {
                            Box::new(ListBuilder::<Float32Builder>::new(Float32Builder::new()))
                        }
                        DataType::Int32 => {
                            Box::new(ListBuilder::<Int32Builder>::new(Int32Builder::new()))
                        }
                        _ => panic!("Unsupported data type {}", child.data_type()),
                    },
                    DataType::FixedSizeList(child, dimension) => match child.data_type() {
                        DataType::Boolean => Box::new(FixedSizeListBuilder::<BooleanBuilder>::new(
                            BooleanBuilder::new(),
                            *dimension,
                        )),
                        DataType::Utf8 => Box::new(FixedSizeListBuilder::<StringBuilder>::new(
                            StringBuilder::new(),
                            *dimension,
                        )),
                        DataType::Float32 => Box::new(FixedSizeListBuilder::<Float32Builder>::new(
                            Float32Builder::new(),
                            *dimension,
                        )),
                        DataType::Int32 => Box::new(FixedSizeListBuilder::<Int32Builder>::new(
                            Int32Builder::new(),
                            *dimension,
                        )),
                        _ => panic!("Unsupported data type {}", child.data_type()),
                    },
                    _ => panic!("Unsupported data type {}", field.data_type()),
                };
                acc.push(builder);
                acc
            });

    let builders: Result<Vec<Box<dyn ArrayBuilder>>> =
        term.into_list_iterator()?
            .try_fold(empty_cols, |mut acc, record| {
                for (idx, field) in schema.fields().into_iter().enumerate() {
                    let val = record.map_get(field.name())?;
                    match field.data_type() {
                        DataType::Boolean => {
                            if let Some(builder) =
                                acc[idx].as_any_mut().downcast_mut::<BooleanBuilder>()
                            {
                                let the_bool: bool = val.decode()?;
                                builder.append_value(the_bool);
                            }
                        }
                        DataType::Utf8 => {
                            if let Some(builder) =
                                acc[idx].as_any_mut().downcast_mut::<StringBuilder>()
                            {
                                let the_str: String = val.decode()?;
                                builder.append_value(the_str);
                            }
                        }
                        DataType::Int32 => {
                            if let Some(builder) =
                                acc[idx].as_any_mut().downcast_mut::<Int32Builder>()
                            {
                                let the_int: i32 = val.decode()?;
                                builder.append_value(the_int);
                            }
                        }
                        DataType::Float32 => {
                            if let Some(builder) =
                                acc[idx].as_any_mut().downcast_mut::<Float32Builder>()
                            {
                                let the_float: f32 = val.decode()?;
                                builder.append_value(the_float);
                            }
                        }
                        DataType::List(child) => match child.data_type() {
                            DataType::Boolean => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<BooleanBuilder>>()
                                {
                                    let the_list: Vec<bool> = val.decode()?;
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            DataType::Utf8 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<StringBuilder>>()
                                {
                                    let the_list: Vec<String> = val.decode()?;
                                    for s in the_list.iter() {
                                        builder.values().append_value(s.as_str());
                                    }
                                    builder.append(true);
                                }
                            }
                            DataType::Int32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<Int32Builder>>()
                                {
                                    let the_list: Vec<i32> = val.decode()?;
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            DataType::Float32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<ListBuilder<Float32Builder>>()
                                {
                                    let the_list: Vec<f32> = val.decode()?;

                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            _ => panic!("Unsupported data type {}", child.data_type()),
                        },
                        DataType::FixedSizeList(child, _dim) => match child.data_type() {
                            DataType::Boolean => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<FixedSizeListBuilder<BooleanBuilder>>()
                                {
                                    let the_list: Vec<bool> = val.decode()?;
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            DataType::Utf8 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<FixedSizeListBuilder<StringBuilder>>()
                                {
                                    let the_list: Vec<String> = val.decode()?;
                                    for s in the_list.iter() {
                                        builder.values().append_value(s.as_str());
                                    }
                                    builder.append(true);
                                }
                            }
                            DataType::Int32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<FixedSizeListBuilder<Int32Builder>>()
                                {
                                    let the_list: Vec<i32> = val.decode()?;
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            DataType::Float32 => {
                                if let Some(builder) = acc[idx]
                                    .as_any_mut()
                                    .downcast_mut::<FixedSizeListBuilder<Float32Builder>>()
                                {
                                    let the_list: Vec<f32> = val.decode()?;
                                    for s in the_list.iter() {
                                        builder.values().append_value(*s);
                                    }
                                    builder.append(true);
                                }
                            }
                            _ => panic!("Unsupported data type {}", child.data_type()),
                        },
                        _ => panic!("Unsupported data type {}", field.data_type()),
                    };
                }
                Ok(acc)
            });

    match builders {
        Ok(mut builders) => {
            let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
            Ok(columns)
        }
        Err(err) => Err(Error::from(err)),
    }
}
