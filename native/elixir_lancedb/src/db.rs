use lancedb::Connection;
use rustler::{Atom, ResourceArc};
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
        Err(_) => Err("failed to create database table".to_string()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn drop_table(conn: ResourceArc<DbConnResource>, table_name: String) -> Result<Atom, String> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async { conn.drop_table(&table_name).await });

    match result {
        Ok(_) => Ok(atoms::tables_dropped()),
        Err(_) => Err("failed to create database table".to_string()),
    }
}

#[rustler::nif(schedule = "DirtyCpu")]
fn create_empty_table(
    conn: ResourceArc<DbConnResource>,
    table_name: String,
    schema: schema::Schema,
) -> Result<Atom, String> {
    let conn = db_conn(conn);
    let result = get_runtime().block_on(async {
        conn.create_empty_table(table_name, Arc::new(schema.into_arrow()))
            .execute()
            .await
    });

    match result {
        Ok(_) => Ok(atoms::created_table()),
        Err(_) => Err("failed to create database table".to_string()),
    }
}

pub fn db_conn(conn_resource: ResourceArc<DbConnResource>) -> Connection {
    let connection;
    {
        connection = conn_resource.0.lock().unwrap().clone();
    }
    connection
}
