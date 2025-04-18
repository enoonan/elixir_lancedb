use std::collections::HashMap;

use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::Schema;
use rustler::{Encoder, Env, Term};

#[derive(Debug)]
pub enum Value {
    Utf8(String),
    Int32(i32),
    // Float32(f32),
    // List(Vec<Primitive>),
    // FixedSizeList(Vec<Primitive>),
}

impl Encoder for Value {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self {
            Value::Utf8(val) => val.encode(env),
            Value::Int32(val) => val.encode(env),
            // Value::Float32(val) => val.encode(env),
            // Value::List(list) => list.encode(env),
            // Value::FixedSizeList(list) => list.encode(env),
        }
    }
}
// #[derive(Debug)]
// enum Primitive {
//     Utf8(String),
//     Int32(i32),
//     Float32(f32),
// }

// impl Encoder for Primitive {
//     fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
//         match &self {
//             Primitive::Utf8(val) => val.encode(env),
//             Primitive::Int32(val) => val.encode(env),
//             Primitive::Float32(val) => val.encode(env),
//         }
//     }
// }

pub fn term_from_arrow(
    results: Vec<RecordBatch>,
    schema: &Schema,
) -> Result<Vec<HashMap<String, Value>>, String> {
    let schema_fields = &schema.fields;
    let empty_recs: Vec<HashMap<String, Value>> = vec![];
    let records: Vec<HashMap<String, Value>> =
        results.into_iter().fold(empty_recs, |mut recs, batch| {
            let num_rows = batch.num_rows();
            let num_columns = batch.num_columns();

            for row_idx in 0..num_rows {
                let mut record: HashMap<String, Value> = HashMap::new();

                for col_idx in 0..num_columns {
                    let field = &schema_fields[col_idx];
                    let column = batch.column(col_idx);
                    let value = match field.data_type() {
                        DataType::Utf8 => {
                            if let Some(string_array) =
                                column.as_any().downcast_ref::<arrow_array::StringArray>()
                            {
                                let val = string_array.value(row_idx);
                                Value::Utf8(val.to_string())
                            } else {
                                Value::Utf8("".to_string())
                            }
                        }
                        DataType::Int32 => {
                            if let Some(int_array) =
                                column.as_any().downcast_ref::<arrow_array::Int32Array>()
                            {
                                let value = int_array.value(row_idx);
                                Value::Int32(value)
                            } else {
                                Value::Int32(0)
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
