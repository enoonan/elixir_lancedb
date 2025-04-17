// use arrow_array::{
//     ffi_stream::ArrowArrayStreamReader, RecordBatch, RecordBatchIterator, RecordBatchReader,
// };

use lancedb::Connection;
use rustler::{Atom, ResourceArc, Term};
use std::sync::{Arc, Mutex};

use crate::{atoms, runtime::get_runtime, schema};

pub struct DbConnResource(pub Arc<Mutex<Connection>>);

#[rustler::nif(schedule = "DirtyCpu")]
fn connect(uri: String) -> Result<ResourceArc<DbConnResource>, String> {
    let result = get_runtime().block_on(async { lancedb::connect(&uri).execute().await });

    match result {
        Ok(conn) => Ok(ResourceArc::new(DbConnResource(Arc::new(Mutex::new(conn))))),
        Err(_) => Err("failed to connect to lanceDB".to_string()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn table_names(conn: ResourceArc<DbConnResource>) -> Result<Vec<String>, String> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async { conn.table_names().execute().await });

    match result {
        Ok(names) => Ok(names),
        Err(_) => Err("failed to create database table".to_string()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_all_tables(conn: ResourceArc<DbConnResource>) -> Result<(), String> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async { conn.drop_all_tables().await });

    match result {
        Ok(_) => Ok(()),
        Err(_) => Err("failed to drop all database tables".to_string()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_table(conn: ResourceArc<DbConnResource>, table_name: String) -> Result<Atom, String> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async { conn.drop_table(&table_name).await });

    match result {
        Ok(_) => Ok(atoms::tables_dropped()),
        Err(_) => Err("failed to drop database table".to_string()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn create_empty_table(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
    schema: schema::Schema,
) -> Result<(), String> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async {
        conn.create_empty_table(table_name, Arc::new(schema.into_arrow()))
            .execute()
            .await
    });

    match result {
        Ok(_) => Ok(()),
        Err(_) => Err("failed to create empty database table".to_string()),
    }
}
#[rustler::nif(schedule = "DirtyCpu")]
fn create_table_with_data(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
    initial_data: Vec<Term>,
    schema: schema::Schema,
) -> Result<(), String> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async {
        conn.create_empty_table(table_name, Arc::new(schema.into_arrow()))
            .execute()
            .await
    });
    println!("{:?}", initial_data);
    match result {
        Ok(_) => Ok(()),
        Err(_) => Err("failed to create database table".to_string()),
    }
}

// #[rustler::nif(schedule = "DirtyCpu")]
// fn create_table(
//     conn: ResourceArc<DbConnResource>,
//     table_name: String,
//     initial_data: Vec<HashMap<Term, Term>>,
// ) -> Result<Atom, String> {
//     let conn = db_conn(conn);
//     let arrow_data = conversion::maps_to_arrow(initial_data);
// let decoded_data = initial_data
//     .iter()
//     .map(|term| {
//         let map: HashMap<String, rustler::Term> = term.decode()?;
//         Ok(map)
//     })
//     .collect::<Result<Vec<_>, rustler::Error>>()
//     .map_err(|_| "failed to decode initial data when creating table");

// let arrow_data = RecordBatch::try_from_iter(arrow_data).unwrap();
// let result = get_runtime()
//     .block_on(async { conn.create_table(table_name, initial_data).execute().await });

// return Ok(atoms::created_table());
// match result {
//     Ok(_) => Ok(atoms::created_table()),
//     Err(_) => Err("failed to create database table".to_string()),
// }
// }

pub fn db_conn(conn_resource: ResourceArc<DbConnResource>) -> Connection {
    let connection;
    {
        connection = conn_resource.0.lock().unwrap().clone();
    }
    connection
}
