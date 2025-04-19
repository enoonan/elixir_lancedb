use crate::error::Error;
use crate::error::Result;
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::Schema;
use rustler::{Encoder, Env, Term};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub enum ReturnValue {
    Utf8(String),
    Int32(i32),
    Float32(f32),
    ListFloat32(Vec<f32>),
    ListInt32(Vec<i32>),
    ListUtf8(Vec<String>),
}

impl Encoder for ReturnValue {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self {
            ReturnValue::Utf8(val) => val.encode(env),
            ReturnValue::Int32(val) => val.encode(env),
            ReturnValue::Float32(val) => val.encode(env),
            ReturnValue::ListFloat32(val) => val.encode(env),
            ReturnValue::ListInt32(val) => val.encode(env),
            ReturnValue::ListUtf8(val) => val.encode(env),
        }
    }
}

pub fn term_from_arrow(
    results: Vec<RecordBatch>,
    schema: &Schema,
) -> Result<Vec<HashMap<String, ReturnValue>>> {
    let schema_fields = &schema.fields;
    let empty_recs: Vec<HashMap<String, ReturnValue>> = vec![];
    let records: Vec<HashMap<String, ReturnValue>> =
        results.into_iter().fold(empty_recs, |mut recs, batch| {
            let num_rows = batch.num_rows();
            let num_columns = batch.num_columns();

            for row_idx in 0..num_rows {
                let mut record: HashMap<String, ReturnValue> = HashMap::new();

                for col_idx in 0..num_columns {
                    let field = &schema_fields[col_idx];
                    let column = batch.column(col_idx);
                    let value = match field.data_type() {
                        DataType::Utf8 => {
                            if let Some(string_array) =
                                column.as_any().downcast_ref::<arrow_array::StringArray>()
                            {
                                let val = string_array.value(row_idx);
                                ReturnValue::Utf8(val.to_string())
                            } else {
                                ReturnValue::Utf8("".to_string())
                            }
                        }
                        DataType::Int32 => {
                            if let Some(int_array) =
                                column.as_any().downcast_ref::<arrow_array::Int32Array>()
                            {
                                let value = int_array.value(row_idx);
                                ReturnValue::Int32(value)
                            } else {
                                ReturnValue::Int32(0)
                            }
                        }
                        DataType::Float32 => {
                            if let Some(float_array) =
                                column.as_any().downcast_ref::<arrow_array::Float32Array>()
                            {
                                let value = float_array.value(row_idx);
                                ReturnValue::Float32(value)
                            } else {
                                ReturnValue::Float32(0.0)
                            }
                        }
                        DataType::List(field) => {
                            if let Some(list_array) =
                                column.as_any().downcast_ref::<arrow_array::ListArray>()
                            {
                                match field.data_type() {
                                    DataType::Utf8 => {
                                        match array_to_values(&list_array.value(row_idx)) {
                                            Ok(value) => value,
                                            Err(_) => ReturnValue::ListUtf8(vec![]),
                                        }
                                    }
                                    DataType::Float32 => {
                                        match array_to_values(&list_array.value(row_idx)) {
                                            Ok(value) => value,
                                            Err(_) => ReturnValue::ListFloat32(vec![]),
                                        }
                                    }
                                    DataType::Int32 => {
                                        match array_to_values(&list_array.value(row_idx)) {
                                            Ok(value) => value,
                                            Err(_) => ReturnValue::ListInt32(vec![]),
                                        }
                                    }
                                    _ => todo!(),
                                }
                            } else {
                                match field.data_type() {
                                    DataType::Utf8 => ReturnValue::ListUtf8(vec![]),
                                    DataType::Int32 => ReturnValue::ListInt32(vec![]),
                                    DataType::Float32 => ReturnValue::ListFloat32(vec![]),
                                    _ => todo!(),
                                }
                            }
                        }
                        DataType::FixedSizeList(field, _d) => {
                            if let Some(list_array) = column
                                .as_any()
                                .downcast_ref::<arrow_array::FixedSizeListArray>()
                            {
                                match field.data_type() {
                                    DataType::Utf8 => {
                                        match array_to_values(&list_array.value(row_idx)) {
                                            Ok(value) => value,
                                            Err(_) => ReturnValue::ListUtf8(vec![]),
                                        }
                                    }
                                    DataType::Float32 => {
                                        match array_to_values(&list_array.value(row_idx)) {
                                            Ok(value) => value,
                                            Err(_) => ReturnValue::ListFloat32(vec![]),
                                        }
                                    }
                                    DataType::Int32 => {
                                        match array_to_values(&list_array.value(row_idx)) {
                                            Ok(value) => value,
                                            Err(_) => ReturnValue::ListInt32(vec![]),
                                        }
                                    }
                                    _ => todo!(),
                                }
                            } else {
                                match field.data_type() {
                                    DataType::Utf8 => ReturnValue::ListUtf8(vec![]),
                                    DataType::Int32 => ReturnValue::ListInt32(vec![]),
                                    DataType::Float32 => ReturnValue::ListFloat32(vec![]),
                                    _ => todo!(),
                                }
                            }
                        }
                        _ => panic!("Unsupported data type: {:?}", field.data_type()),
                    };
                    record.insert(field.name().to_string(), value);
                }
                recs.push(record);
            }
            recs
        });
    Ok(records)
}

fn array_to_values(array: &Arc<dyn Array>) -> Result<ReturnValue> {
    // Check the data type and downcast accordingly
    match array.data_type() {
        DataType::Int32 => {
            let typed_array = array
                .as_any()
                .downcast_ref::<arrow_array::Int32Array>()
                .ok_or_else(|| "Failed to downcast to Int32Array".to_string())?;

            let values: Vec<i32> = typed_array
                .iter()
                .map(|opt_val| opt_val.unwrap_or(0)) // Handle nulls
                .collect();

            Ok(ReturnValue::ListInt32(values))
        }

        DataType::Float32 => {
            let typed_array = array
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .ok_or_else(|| "Failed to downcast to Float32Array".to_string())?;

            let values: Vec<f32> = typed_array
                .iter()
                .map(|opt_val| opt_val.unwrap_or(0.0)) // Handle nulls
                .collect();

            Ok(ReturnValue::ListFloat32(values))
        }

        DataType::Utf8 => {
            let typed_array = array
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| "Failed to downcast to StringArray".to_string())?;

            let values: Vec<String> = typed_array
                .iter()
                .map(|opt_val| opt_val.map(|s| s.to_string()).unwrap_or_default())
                .collect();

            Ok(ReturnValue::ListUtf8(values))
        }

        // Add more type handling as needed
        _ => Err(Error::InvalidInput {
            message: format!("Unsupported data type: {}", array.data_type()),
        }),
    }
}
