use arrow_array::{RecordBatch, RecordBatchIterator};
use lancedb::Connection;
use rustler::{Atom, ResourceArc, Term};
use std::sync::{Arc, Mutex};

use crate::{atoms, conversion, runtime::get_runtime, schema};

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
    erl_data: Term,
    erl_schema: schema::Schema,
) -> Result<(), String> {
    let arrow_schema = Arc::new(erl_schema.clone().into_arrow());
    let columnar_data = conversion::to_arrow(erl_data, erl_schema.clone()).unwrap();
    let batch = RecordBatchIterator::new(
        vec![RecordBatch::try_new(arrow_schema.clone(), columnar_data)],
        arrow_schema.clone(),
    );

    let conn = db_conn(conn);
    let result = get_runtime().block_on(async {
        conn.create_table(table_name, Box::new(batch))
            .execute()
            .await
    });
    match result {
        Ok(_) => Ok(()),
        Err(_) => {
            return Err("failed to create database table from data".to_string());
        }
    }
}

// #[rustler::nif(schedule = "DirtyCpu")]
// fn query_table<'a>(
//     conn: ResourceArc<DbConnResource>,
//     table_name: String,
// ) -> Result<Term<'a>, String> {

// }

pub fn db_conn(conn_resource: ResourceArc<DbConnResource>) -> Connection {
    let connection;
    {
        connection = conn_resource.0.lock().unwrap().clone();
    }
    connection
}
